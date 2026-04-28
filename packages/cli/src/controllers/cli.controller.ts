import { Logger } from '@n8n/backend-common';
import {
	WorkflowRepository,
	SharedWorkflowRepository,
	ProjectRepository,
	ExecutionRepository,
	generateNanoId,
} from '@n8n/db';
import { Get, Post, RestController } from '@n8n/decorators';
import type { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import type { IWorkflowBase, IWorkflowExecutionDataProcess, IDataObject } from 'n8n-workflow';
import { CHAT_TRIGGER_NODE_TYPE, createRunExecutionData } from 'n8n-workflow';

import { ActiveExecutions } from '@/active-executions';
import { OwnershipService } from '@/services/ownership.service';
import { isWorkflowIdValid } from '@/utils';
import { WorkflowRunner } from '@/workflow-runner';

interface CliRunBody {
	workflowData: IWorkflowBase;
	chatInput?: string;
	/** Arbitrary input data to inject into the trigger node (for executeWorkflowTrigger etc.) */
	inputData?: IDataObject;
	/** Actual file modification time from the filesystem (ISO string) */
	fileModifiedAt?: string;
}

/**
 * Internal CLI controller for executing workflows from the command line.
 * All endpoints skip authentication (skipAuth: true) and are restricted to
 * localhost requests only for security.
 */
@RestController('/cli')
export class CliController {
	private static readonly MAX_WAIT_MS = 5 * 60 * 1000; // 5 minutes

	constructor(
		private readonly logger: Logger,
		private readonly workflowRepository: WorkflowRepository,
		private readonly sharedWorkflowRepository: SharedWorkflowRepository,
		private readonly projectRepository: ProjectRepository,
		private readonly executionRepository: ExecutionRepository,
		private readonly ownershipService: OwnershipService,
		private readonly activeExecutions: ActiveExecutions,
		private readonly workflowRunner: WorkflowRunner,
	) {}

	/**
	 * Check if a trigger node is a webhook-based trigger that needs pinData injection.
	 */
	private isWebhookBasedTrigger(nodeType: string): boolean {
		return (
			nodeType === CHAT_TRIGGER_NODE_TYPE ||
			nodeType === 'n8n-nodes-base.webhook' ||
			nodeType.toLowerCase().includes('webhook')
		);
	}

	/**
	 * Ensure localhost-only access.
	 */
	private isLocalRequest(req: Request): boolean {
		const remoteAddress = req.ip || req.socket.remoteAddress || '';
		return (
			remoteAddress === '127.0.0.1' ||
			remoteAddress === '::1' ||
			remoteAddress === '::ffff:127.0.0.1' ||
			remoteAddress === 'localhost'
		);
	}

	/**
	 * Synchronous workflow run endpoint.
	 * Accepts a full workflow JSON + optional chatInput, syncs it to the DB,
	 * executes it, waits for completion, and returns the full results.
	 *
	 * POST /rest/cli/run
	 */
	@Post('/run', { skipAuth: true })
	async run(req: Request, res: Response) {
		const startTime = Date.now();
		this.logger.info('[cli] ── RUN REQUEST ──');

		if (!this.isLocalRequest(req)) {
			res.status(403).json({ error: 'CLI endpoint is only accessible from localhost' });
			return;
		}

		const body = req.body as CliRunBody;

		if (!body.workflowData) {
			res.status(400).json({ error: 'Missing workflowData in request body' });
			return;
		}

		const fileData = body.workflowData;
		const chatInput = body.chatInput;
		const inputData = body.inputData;
		const fileModifiedAt = body.fileModifiedAt;

		this.logger.info(`[cli] inputData: ${inputData ? JSON.stringify(inputData) : '(none)'}`);

		// Validate basic workflow structure
		if (!fileData.nodes || !Array.isArray(fileData.nodes)) {
			res.status(400).json({ error: 'Workflow does not contain valid nodes' });
			return;
		}

		if (!fileData.connections || typeof fileData.connections !== 'object') {
			res.status(400).json({ error: 'Workflow does not contain valid connections' });
			return;
		}

		this.logger.info(`[cli] Workflow: "${fileData.name}", Nodes: ${fileData.nodes.length}`);

		try {
			// ── Step 1: Sync workflow to DB ──────────────────────────────────
			const user = await this.ownershipService.getInstanceOwner();
			const syncResult = await this.syncWorkflow(fileData, user.id, fileModifiedAt);
			const workflow = await this.workflowRepository.findOneBy({ id: syncResult.id });

			if (!workflow) {
				res.status(500).json({ error: 'Failed to sync workflow to database' });
				return;
			}

			this.logger.info(`[cli] Synced workflow: "${workflow.name}" (ID: ${syncResult.id})`);

			// ── Step 2: Find trigger node ────────────────────────────────────
			const triggerNode = workflow.nodes.find(
				(node) =>
					node.type.toLowerCase().includes('trigger') ||
					node.type.toLowerCase().includes('webhook') ||
					node.type === 'n8n-nodes-base.start',
			);

			if (!triggerNode) {
				res.status(400).json({ error: 'No trigger node found in workflow' });
				return;
			}

			this.logger.info(`[cli] Trigger: "${triggerNode.name}" (${triggerNode.type})`);

			// ── Step 3: Execute workflow ─────────────────────────────────────
			let executionId: string;

			if (this.isWebhookBasedTrigger(triggerNode.type)) {
				// Webhook/chat triggers: inject mock data via pinData + nodeExecutionStack
				this.logger.info('[cli] Using direct execution (webhook/chat trigger)');

				const isChatTrigger = triggerNode.type === CHAT_TRIGGER_NODE_TYPE;
				const mockData = isChatTrigger
					? {
							sessionId: `cli-${Date.now()}`,
							action: 'sendMessage',
							chatInput: chatInput ?? 'CLI execution',
						}
					: {
							headers: {},
							params: {},
							query: {},
							body: { chatInput },
						};

				const executionData = createRunExecutionData({
					startData: {},
					resultData: {
						pinData: { [triggerNode.name]: [{ json: mockData }] },
						runData: {},
					},
					executionData: {
						contextData: {},
						metadata: {},
						nodeExecutionStack: [
							{
								node: triggerNode,
								data: {
									main: [[{ json: mockData }]],
								},
								source: null,
							},
						],
						waitingExecution: {},
						waitingExecutionSource: {},
					},
				});

				const runData: IWorkflowExecutionDataProcess = {
					executionMode: isChatTrigger ? 'chat' : 'webhook',
					workflowData: workflow,
					userId: user.id,
					startNodes: [{ name: triggerNode.name, sourceData: null }],
					pinData: { [triggerNode.name]: [{ json: mockData }] },
					executionData,
				};

				executionId = await this.workflowRunner.run(runData);
			} else {
				// Regular triggers: inject input data via nodeExecutionStack
				// (NOT pinData — pinData skips the node's execute(), losing schema processing)
				this.logger.info('[cli] Using trigger execution (regular trigger)');

				// Build trigger input data from inputData or chatInput
				const triggerData: IDataObject = (inputData ?? {}) as IDataObject;
				if (chatInput !== undefined && !inputData) {
					triggerData.chatInput = chatInput;
				}

				this.logger.info(`[cli] Trigger input data: ${JSON.stringify(triggerData)}`);

				const executionData = createRunExecutionData({
					startData: {},
					resultData: {
						pinData: { [triggerNode.name]: [{ json: triggerData }] },
						runData: {},
					},
					executionData: {
						contextData: {},
						metadata: {},
						nodeExecutionStack: [
							{
								node: triggerNode,
								data: {
									main: [[{ json: triggerData }]],
								},
								source: null,
							},
						],
						waitingExecution: {},
						waitingExecutionSource: {},
					},
				});

				const runData: IWorkflowExecutionDataProcess = {
					executionMode: 'trigger',
					workflowData: workflow,
					userId: user.id,
					startNodes: [{ name: triggerNode.name, sourceData: null }],
					pinData: { [triggerNode.name]: [{ json: triggerData }] },
					executionData,
				};

				executionId = await this.workflowRunner.run(runData);
			}

			this.logger.info(`[cli] Execution started: ${executionId}`);

			// ── Step 4: Wait for completion ──────────────────────────────────
			let timeoutId: NodeJS.Timeout | undefined;
			const timeoutPromise = new Promise<never>((_, reject) => {
				timeoutId = setTimeout(() => {
					reject(new Error(`Execution timed out after ${CliController.MAX_WAIT_MS / 1000}s`));
				}, CliController.MAX_WAIT_MS);
			});

			try {
				const runResult = await Promise.race([
					this.activeExecutions.getPostExecutePromise(executionId),
					timeoutPromise,
				]);

				if (timeoutId) clearTimeout(timeoutId);

				const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

				if (!runResult) {
					this.logger.error('[cli] Execution returned no data');
					res.json({
						success: false,
						executionId,
						status: 'error',
						executionTime: totalTime,
						error: 'Execution returned no data',
					});
					return;
				}

				const isSuccess = runResult.status !== 'error' && !runResult.data.resultData?.error;
				this.logger.info(
					`[cli] Execution completed in ${totalTime}s — status: ${runResult.status}, success: ${isSuccess}`,
				);

				// Also fetch full persisted execution data for complete node outputs
				const fullExecution = await this.executionRepository.findSingleExecution(executionId, {
					includeData: true,
					unflattenData: true,
				});

				const responsePayload: Record<string, unknown> = {
					success: isSuccess,
					executionId,
					status: runResult.status,
					executionTime: totalTime,
					data: fullExecution?.data?.resultData ?? runResult.data.resultData,
				};

				// If server had a newer workflow, include it so the CLI can update the file
				if (syncResult.syncedWorkflow) {
					responsePayload.syncedWorkflow = syncResult.syncedWorkflow;
				}

				res.json(responsePayload);
			} catch (error) {
				if (timeoutId) clearTimeout(timeoutId);
				throw error;
			}
		} catch (error) {
			const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
			this.logger.error(`[cli] Error: ${error instanceof Error ? error.message : String(error)}`);
			res.status(500).json({
				success: false,
				executionTime: totalTime,
				error: error instanceof Error ? error.message : String(error),
			});
		}
	}

	/**
	 * Sync workflow JSON to the database. Matches by ID, then by name, or creates new.
	 * Returns the workflow ID and optionally the server workflow data for syncing back to file.
	 */
	private async syncWorkflow(
		fileData: IWorkflowBase,
		userId: string,
		fileModifiedAt?: string,
	): Promise<{ id: string; syncedWorkflow?: Record<string, unknown> }> {
		// Try matching by ID
		if (fileData.id && isWorkflowIdValid(fileData.id)) {
			const existing = await this.workflowRepository.findOneBy({ id: fileData.id });
			if (existing) {
				const fileUpdatedAt = fileModifiedAt ? new Date(fileModifiedAt) : null;
				const serverUpdatedAt = existing.updatedAt
					? new Date(existing.updatedAt as unknown as string)
					: null;

				this.logger.info(
					`[cli] Found existing workflow (ID match): ${existing.id}, ` +
						`fileUpdatedAt=${fileUpdatedAt?.toISOString() ?? 'null'}, ` +
						`serverUpdatedAt=${serverUpdatedAt?.toISOString() ?? 'null'}`,
				);

				// Update unless server is strictly newer than file
				const shouldUpdate = !fileUpdatedAt || !serverUpdatedAt || fileUpdatedAt >= serverUpdatedAt;

				if (shouldUpdate) {
					this.logger.info(`[cli] Updating existing workflow (ID match): ${existing.id}`);
					await this.workflowRepository.update(existing.id, {
						nodes: fileData.nodes,
						connections: fileData.connections,
						settings: fileData.settings,
						name: fileData.name,
						updatedAt: fileModifiedAt ? new Date(fileModifiedAt) : new Date(),
					});
					return { id: existing.id };
				} else {
					this.logger.info(
						`[cli] Using existing workflow (ID match): ${existing.id} ` +
							`(server is newer: ${serverUpdatedAt!.toISOString()} > ${fileUpdatedAt!.toISOString()})`,
					);
					return {
						id: existing.id,
						syncedWorkflow: {
							name: existing.name,
							nodes: existing.nodes,
							connections: existing.connections,
							settings: existing.settings,
							pinData: existing.pinData ?? {},
						},
					};
				}
			}
		}

		// Try matching by name
		if (fileData.name) {
			const existing = await this.workflowRepository.findOneBy({ name: fileData.name });
			if (existing) {
				const fileUpdatedAt = fileModifiedAt ? new Date(fileModifiedAt) : null;
				const serverUpdatedAt = existing.updatedAt
					? new Date(existing.updatedAt as unknown as string)
					: null;

				this.logger.info(
					`[cli] Found existing workflow (name match): ${existing.id}, ` +
						`fileUpdatedAt=${fileUpdatedAt?.toISOString() ?? 'null'}, ` +
						`serverUpdatedAt=${serverUpdatedAt?.toISOString() ?? 'null'}`,
				);

				// Update unless server is strictly newer than file
				const shouldUpdate = !fileUpdatedAt || !serverUpdatedAt || fileUpdatedAt >= serverUpdatedAt;

				if (shouldUpdate) {
					this.logger.info(`[cli] Updating existing workflow (name match): ${existing.id}`);
					await this.workflowRepository.update(existing.id, {
						nodes: fileData.nodes,
						connections: fileData.connections,
						settings: fileData.settings,
						name: fileData.name,
						updatedAt: fileModifiedAt ? new Date(fileModifiedAt) : new Date(),
					});
					return { id: existing.id };
				} else {
					this.logger.info(
						`[cli] Using existing workflow (name match): ${existing.id} ` +
							`(server is newer: ${serverUpdatedAt!.toISOString()} > ${fileUpdatedAt!.toISOString()})`,
					);
					return {
						id: existing.id,
						syncedWorkflow: {
							name: existing.name,
							nodes: existing.nodes,
							connections: existing.connections,
							settings: existing.settings,
							pinData: existing.pinData ?? {},
						},
					};
				}
			}
		}

		// Create new workflow
		const workflowId =
			fileData.id && isWorkflowIdValid(fileData.id) ? fileData.id : generateNanoId();
		fileData.id = workflowId;
		this.logger.info(`[cli] Creating new workflow: "${fileData.name}" (ID: ${workflowId})`);

		const { manager: dbManager } = this.workflowRepository;
		await dbManager.transaction(async (transactionManager) => {
			const personalProject = await this.projectRepository.getPersonalProjectForUserOrFail(
				userId,
				transactionManager,
			);

			const workflowEntity = this.workflowRepository.create({
				...fileData,
				active: false,
				isArchived: false,
				versionId: fileData.versionId || uuidv4(),
				createdAt: fileData.createdAt || new Date(),
				updatedAt: fileData.updatedAt || new Date(),
			});

			const savedWorkflow = await transactionManager.save(workflowEntity);

			const sharedWorkflow = this.sharedWorkflowRepository.create({
				role: 'workflow:owner',
				projectId: personalProject.id,
				workflow: savedWorkflow,
			});

			await transactionManager.save(sharedWorkflow);
		});

		return { id: workflowId };
	}

	/**
	 * Health check endpoint for the CLI to verify server connectivity.
	 * GET /rest/cli/health
	 */
	@Get('/health', { skipAuth: true })
	async health() {
		return { status: 'ok', timestamp: new Date().toISOString() };
	}
}
