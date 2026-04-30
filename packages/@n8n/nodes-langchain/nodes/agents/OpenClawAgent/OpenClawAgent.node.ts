import { spawn, type ChildProcess } from 'child_process';
import { randomUUID } from 'crypto';
import { existsSync, statSync } from 'fs';
import { delimiter, dirname, join } from 'path';

import {
	NodeConnectionTypes,
	NodeOperationError,
	jsonParse,
	type IDataObject,
	type IExecuteFunctions,
	type INodeExecutionData,
	type INodeType,
	type INodeTypeDescription,
} from 'n8n-workflow';

interface OpenClawProcessResult {
	stdout: string;
	stderr: string;
	exitCode: number | null;
	signal: NodeJS.Signals | null;
	command: string;
}

interface ResolvedBinary {
	binaryPath: string;
	pathDirectories: string[];
}

type SelectorType = 'agent' | 'sessionId' | 'recipient' | 'default';

const selectorTypeToParameterName: Record<Exclude<SelectorType, 'default'>, string> = {
	agent: 'agentId',
	sessionId: 'sessionId',
	recipient: 'to',
};

const selectorTypeToCliFlag: Record<Exclude<SelectorType, 'default'>, string> = {
	agent: '--agent',
	sessionId: '--session-id',
	recipient: '--to',
};

const DEFAULT_TIMEOUT_SECONDS = 300;
const CLI_SHUTDOWN_GRACE_SECONDS = 90;

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function getErrorMessage(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}

	return String(error);
}

function normalizeOptionalString(value: unknown): string | undefined {
	if (typeof value !== 'string') {
		return undefined;
	}

	const trimmed = value.trim();
	return trimmed || undefined;
}

function parseOpenClawOutput(stdout: string): IDataObject {
	const trimmed = stdout.trim();
	if (!trimmed) {
		return {};
	}

	const parsed = jsonParse<unknown>(trimmed);
	if (isObject(parsed)) {
		return parsed as IDataObject;
	}

	return { result: parsed as object };
}

function isUsableFile(filePath: string): boolean {
	try {
		return existsSync(filePath) && statSync(filePath).isFile();
	} catch {
		return false;
	}
}

function getHomeDirectory(): string | undefined {
	return (
		normalizeOptionalString(process.env.HOME) ?? normalizeOptionalString(process.env.USERPROFILE)
	);
}

function getDefaultBinarySearchPaths(binaryName: string): string[] {
	const home = getHomeDirectory();
	const homeCandidates = home
		? [
				join(home, '.volta', 'bin', binaryName),
				join(home, '.local', 'bin', binaryName),
				join(home, '.npm-global', 'bin', binaryName),
				join(home, '.bun', 'bin', binaryName),
				join(home, 'Library', 'pnpm', binaryName),
			]
		: [];

	return [
		normalizeOptionalString(process.env.OPENCLAW_BINARY_PATH),
		normalizeOptionalString(process.env.OPENCLAW_BIN),
		...(process.env.PATH ?? '')
			.split(delimiter)
			.filter(Boolean)
			.map((pathDirectory) => join(pathDirectory, binaryName)),
		...homeCandidates,
		join('/opt/homebrew/bin', binaryName),
		join('/usr/local/bin', binaryName),
	].filter((candidate): candidate is string => typeof candidate === 'string');
}

function resolveOpenClawBinary(binaryPath: string): ResolvedBinary {
	if (binaryPath.includes('/') || binaryPath.includes('\\')) {
		return {
			binaryPath,
			pathDirectories: [dirname(binaryPath)],
		};
	}

	for (const candidate of getDefaultBinarySearchPaths(binaryPath)) {
		if (isUsableFile(candidate)) {
			return {
				binaryPath: candidate,
				pathDirectories: [dirname(candidate)],
			};
		}
	}

	return {
		binaryPath,
		pathDirectories: [],
	};
}

function createOpenClawProcessEnv(pathDirectories: string[]): NodeJS.ProcessEnv {
	const existingPath = process.env.PATH ?? '';
	const prependedPath = pathDirectories.filter(Boolean).join(delimiter);
	const nextPath = prependedPath ? `${prependedPath}${delimiter}${existingPath}` : existingPath;

	return {
		...process.env,
		PATH: nextPath,
	};
}

function killOpenClawProcess(child: ChildProcess, signal: NodeJS.Signals): void {
	try {
		if (child.pid && process.platform !== 'win32') {
			process.kill(-child.pid, signal);
			return;
		}
	} catch {
		// Fall back to killing only the direct child below.
	}

	try {
		child.kill(signal);
	} catch {
		// The process may have exited between timeout and signal delivery.
	}
}

function summarizeProcessOutput(output: string): string {
	const trimmed = output.trim();
	if (!trimmed) {
		return '';
	}

	const maxLength = 2000;
	if (trimmed.length <= maxLength) {
		return trimmed;
	}

	return `${trimmed.slice(0, maxLength)}...`;
}

function getWatchdogTimeoutMs(timeoutSeconds: number): number {
	// OpenClaw may use the CLI timeout for the agent turn while still needing time
	// to return the gateway result and clean up local resources.
	return (timeoutSeconds + CLI_SHUTDOWN_GRACE_SECONDS) * 1000;
}

function getGatewayCallArgs(params: IDataObject, rpcTimeoutMs: number): string[] {
	return [
		'gateway',
		'call',
		'agent',
		'--expect-final',
		'--json',
		'--timeout',
		String(rpcTimeoutMs),
		'--params',
		JSON.stringify(params),
	];
}

function quoteCommandArgument(value: string): string {
	if (/^[A-Za-z0-9_/:=-]+$/.test(value)) {
		return value;
	}

	return `'${value.replaceAll("'", "'\\''")}'`;
}

function getOpenClawCommand(binaryPath: string, args: string[]): string {
	return [binaryPath, ...args].map(quoteCommandArgument).join(' ');
}

async function runOpenClawCli(params: {
	binaryPath: string;
	args: string[];
	cwd?: string;
	timeoutMs: number;
	abortSignal?: AbortSignal;
}): Promise<OpenClawProcessResult> {
	return await new Promise<OpenClawProcessResult>((resolve, reject) => {
		const resolvedBinary = resolveOpenClawBinary(params.binaryPath);
		const command = getOpenClawCommand(resolvedBinary.binaryPath, params.args);
		const child = spawn(resolvedBinary.binaryPath, params.args, {
			cwd: params.cwd,
			detached: process.platform !== 'win32',
			stdio: ['ignore', 'pipe', 'pipe'],
			env: createOpenClawProcessEnv(resolvedBinary.pathDirectories),
		});

		let stdout = '';
		let stderr = '';
		let aborted = false;
		let timedOut = false;
		let forceKillTimer: NodeJS.Timeout | undefined;

		const abortHandler = () => {
			aborted = true;
			killOpenClawProcess(child, 'SIGTERM');
		};

		const timeoutTimer = setTimeout(() => {
			timedOut = true;
			killOpenClawProcess(child, 'SIGTERM');
			forceKillTimer = setTimeout(() => {
				killOpenClawProcess(child, 'SIGKILL');
			}, 5000);
		}, params.timeoutMs);

		params.abortSignal?.addEventListener('abort', abortHandler, { once: true });

		child.stdout.on('data', (data: Buffer) => {
			stdout += data.toString();
		});

		child.stderr.on('data', (data: Buffer) => {
			stderr += data.toString();
		});

		child.on('error', (error: Error) => {
			clearTimeout(timeoutTimer);
			if (forceKillTimer) {
				clearTimeout(forceKillTimer);
			}
			params.abortSignal?.removeEventListener('abort', abortHandler);
			reject(
				new Error(
					`Failed to spawn OpenClaw CLI at "${resolvedBinary.binaryPath}": ${error.message}. Set Options > Binary Path to the full path of the openclaw executable, or set OPENCLAW_BINARY_PATH in the n8n process environment.`,
				),
			);
		});

		child.on('close', (exitCode: number | null, signal: NodeJS.Signals | null) => {
			clearTimeout(timeoutTimer);
			if (forceKillTimer) {
				clearTimeout(forceKillTimer);
			}
			params.abortSignal?.removeEventListener('abort', abortHandler);

			if (aborted) {
				reject(new Error('OpenClaw CLI execution was cancelled'));
				return;
			}

			if (timedOut) {
				const stderrSummary = summarizeProcessOutput(stderr);
				const stdoutSummary = summarizeProcessOutput(stdout);
				const details =
					stderrSummary || stdoutSummary ? ` Last output: ${stderrSummary || stdoutSummary}` : '';
				reject(
					new Error(
						`OpenClaw CLI did not finish within ${Math.ceil(params.timeoutMs / 1000)} seconds and was stopped.${details}`,
					),
				);
				return;
			}

			resolve({ stdout, stderr, exitCode, signal, command });
		});
	});
}

export class OpenClawAgent implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenClaw AI Agent',
		name: 'openClawAgent',
		icon: 'file:openclaw.svg',
		group: ['transform'],
		version: 1,
		description: 'Runs a one-shot OpenClaw agent turn through the OpenClaw CLI',
		subtitle:
			'={{$parameter.selectorType === "agent" ? "Agent: " + $parameter.agentId : $parameter.selectorType === "sessionId" ? "Session: " + $parameter.sessionId : $parameter.selectorType === "recipient" ? "To: " + $parameter.to : "Default route"}}',
		defaults: {
			name: 'OpenClaw AI Agent',
		},
		codex: {
			alias: ['OpenClaw', 'Agent', 'Gateway', 'Assistant'],
			categories: ['AI'],
			subcategories: {
				AI: ['Agents', 'Root Nodes'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://docs.openclaw.ai/cli/agent',
					},
				],
			},
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		properties: [
			{
				displayName:
					'Requires the OpenClaw CLI to be installed and configured on the n8n host. The node runs <code>openclaw agent --json</code> and returns OpenClaw payloads and metadata.',
				name: 'openClawNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Message',
				name: 'message',
				type: 'string',
				required: true,
				default: '={{ $json.chatInput || $json.chat_input || $json.message || $json.text || "" }}',
				description: 'Message body to send to the OpenClaw agent',
				typeOptions: {
					rows: 5,
				},
			},
			{
				displayName: 'Route By',
				name: 'selectorType',
				type: 'options',
				default: 'agent',
				noDataExpression: true,
				description: 'How to target the OpenClaw agent turn',
				options: [
					{
						name: 'Agent ID',
						value: 'agent',
						description: 'Run against a configured OpenClaw agent',
					},
					{
						name: 'Existing Session ID',
						value: 'sessionId',
						description: 'Continue an existing OpenClaw session',
					},
					{
						name: 'Recipient',
						value: 'recipient',
						description: 'Use a recipient/channel target to derive the session',
					},
					{
						name: 'OpenClaw Default',
						value: 'default',
						description: 'Let OpenClaw choose its default route',
					},
				],
			},
			{
				displayName: 'Agent ID',
				name: 'agentId',
				type: 'string',
				default: 'main',
				description: 'Configured OpenClaw agent ID',
				displayOptions: {
					show: {
						selectorType: ['agent'],
					},
				},
			},
			{
				displayName: 'Session ID',
				name: 'sessionId',
				type: 'string',
				default: '',
				description: 'OpenClaw session ID to continue',
				displayOptions: {
					show: {
						selectorType: ['sessionId'],
					},
				},
			},
			{
				displayName: 'Recipient',
				name: 'to',
				type: 'string',
				default: '',
				description: 'Recipient or channel target passed to OpenClaw as --to',
				displayOptions: {
					show: {
						selectorType: ['recipient'],
					},
				},
			},
			{
				displayName: 'Model',
				name: 'model',
				type: 'string',
				default: 'openai-codex/gpt-5.5',
				description:
					'Model override for this run. Use an OpenClaw model reference such as openai-codex/gpt-5.5.',
			},
			{
				displayName: 'Thinking Level',
				name: 'thinking',
				type: 'options',
				default: '',
				description: 'Optional OpenClaw thinking level override for this run',
				options: [
					{ name: 'Adaptive', value: 'adaptive' },
					{ name: 'Extra High', value: 'xhigh' },
					{ name: 'High', value: 'high' },
					{ name: 'Low', value: 'low' },
					{ name: 'Max', value: 'max' },
					{ name: 'Medium', value: 'medium' },
					{ name: 'Minimal', value: 'minimal' },
					{ name: 'Off', value: 'off' },
					{ name: 'Use OpenClaw Default', value: '' },
				],
			},
			{
				displayName: 'Run Locally',
				name: 'local',
				type: 'boolean',
				default: false,
				description: 'Whether to force OpenClaw embedded local runtime instead of Gateway mode',
			},
			{
				displayName: 'Deliver Reply',
				name: 'deliver',
				type: 'boolean',
				default: false,
				description:
					'Whether OpenClaw should deliver the reply back to the selected channel/target',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'System Message',
						name: 'systemMessage',
						type: 'string',
						default: '',
						description:
							'Additional system instructions for this OpenClaw run. When set, the node sends the run through the OpenClaw Gateway agent RPC so the instructions are passed as extraSystemPrompt.',
						typeOptions: {
							rows: 5,
						},
					},
					{
						displayName: 'Binary Path',
						name: 'binaryPath',
						type: 'string',
						default: 'openclaw',
						description: 'Path to the openclaw binary. Defaults to "openclaw" in PATH.',
					},
					{
						displayName: 'Working Directory',
						name: 'workingDirectory',
						type: 'string',
						default: '',
						description:
							'Working directory for the OpenClaw process. Leave empty to use n8n default.',
					},
					{
						displayName: 'Timeout',
						name: 'timeout',
						type: 'number',
						default: DEFAULT_TIMEOUT_SECONDS,
						description:
							'OpenClaw agent timeout in seconds. The node allows extra time for the CLI to return the final gateway result before stopping the process.',
						typeOptions: {
							minValue: 1,
						},
					},
					{
						displayName: 'Channel',
						name: 'channel',
						type: 'string',
						default: '',
						description: 'Delivery channel passed to OpenClaw as --channel',
					},
					{
						displayName: 'Reply To',
						name: 'replyTo',
						type: 'string',
						default: '',
						description: 'Delivery target override passed to OpenClaw as --reply-to',
					},
					{
						displayName: 'Reply Channel',
						name: 'replyChannel',
						type: 'string',
						default: '',
						description: 'Delivery channel override passed to OpenClaw as --reply-channel',
					},
					{
						displayName: 'Reply Account',
						name: 'replyAccount',
						type: 'string',
						default: '',
						description: 'Delivery account ID override passed to OpenClaw as --reply-account',
					},
					{
						displayName: 'Verbose',
						name: 'verbose',
						type: 'options',
						default: '',
						description: 'Optional OpenClaw verbose setting to persist for the session',
						options: [
							{ name: 'Full', value: 'full' },
							{ name: 'Leave Unchanged', value: '' },
							{ name: 'Off', value: 'off' },
							{ name: 'On', value: 'on' },
						],
					},
					{
						displayName: 'Include Raw Output',
						name: 'includeRawOutput',
						type: 'boolean',
						default: false,
						description:
							'Whether to include raw stdout and stderr from the OpenClaw CLI in the output',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				const message = normalizeOptionalString(this.getNodeParameter('message', itemIndex));
				if (!message) {
					throw new NodeOperationError(this.getNode(), 'Message must not be empty', { itemIndex });
				}

				let args = ['agent', '--message', message, '--json'];
				let processTimeoutMs: number | undefined;
				const gatewayParams: IDataObject = {
					message,
					idempotencyKey: randomUUID(),
				};
				const selectorType = this.getNodeParameter('selectorType', itemIndex) as SelectorType;
				if (selectorType !== 'default') {
					const parameterName = selectorTypeToParameterName[selectorType];
					const value = normalizeOptionalString(this.getNodeParameter(parameterName, itemIndex));
					if (!value) {
						throw new NodeOperationError(
							this.getNode(),
							`${parameterName} must not be empty when Route By is ${selectorType}`,
							{ itemIndex },
						);
					}
					args.push(selectorTypeToCliFlag[selectorType], value);
					if (selectorType === 'agent') {
						gatewayParams.agentId = value;
					} else if (selectorType === 'sessionId') {
						gatewayParams.sessionId = value;
					} else {
						gatewayParams.to = value;
					}
				}

				const model = normalizeOptionalString(this.getNodeParameter('model', itemIndex));
				if (model) {
					args.push('--model', model);
					gatewayParams.model = model;
				}

				const thinking = normalizeOptionalString(this.getNodeParameter('thinking', itemIndex));
				if (thinking) {
					args.push('--thinking', thinking);
					gatewayParams.thinking = thinking;
				}

				if (this.getNodeParameter('local', itemIndex, false) === true) {
					args.push('--local');
				}

				if (this.getNodeParameter('deliver', itemIndex, false) === true) {
					args.push('--deliver');
					gatewayParams.deliver = true;
				}

				const timeout = Number(
					this.getNodeParameter('options.timeout', itemIndex, DEFAULT_TIMEOUT_SECONDS),
				);
				const timeoutSeconds =
					Number.isFinite(timeout) && timeout > 0 ? Math.floor(timeout) : DEFAULT_TIMEOUT_SECONDS;
				args.push('--timeout', String(timeoutSeconds));
				gatewayParams.timeout = timeoutSeconds;

				const channel = normalizeOptionalString(
					this.getNodeParameter('options.channel', itemIndex, ''),
				);
				if (channel) {
					args.push('--channel', channel);
					gatewayParams.channel = channel;
				}

				const replyTo = normalizeOptionalString(
					this.getNodeParameter('options.replyTo', itemIndex, ''),
				);
				if (replyTo) {
					args.push('--reply-to', replyTo);
					gatewayParams.replyTo = replyTo;
				}

				const replyChannel = normalizeOptionalString(
					this.getNodeParameter('options.replyChannel', itemIndex, ''),
				);
				if (replyChannel) {
					args.push('--reply-channel', replyChannel);
					gatewayParams.replyChannel = replyChannel;
				}

				const replyAccount = normalizeOptionalString(
					this.getNodeParameter('options.replyAccount', itemIndex, ''),
				);
				if (replyAccount) {
					args.push('--reply-account', replyAccount);
					gatewayParams.replyAccountId = replyAccount;
				}

				const verbose = normalizeOptionalString(
					this.getNodeParameter('options.verbose', itemIndex, ''),
				);
				if (verbose) {
					args.push('--verbose', verbose);
				}

				const binaryPath =
					normalizeOptionalString(this.getNodeParameter('options.binaryPath', itemIndex, '')) ??
					'openclaw';
				const workingDirectory = normalizeOptionalString(
					this.getNodeParameter('options.workingDirectory', itemIndex, ''),
				);

				if (
					workingDirectory &&
					(!existsSync(workingDirectory) || !statSync(workingDirectory).isDirectory())
				) {
					throw new NodeOperationError(
						this.getNode(),
						`Working directory does not exist or is not a directory: ${workingDirectory}`,
						{ itemIndex },
					);
				}

				const systemMessage = normalizeOptionalString(
					this.getNodeParameter('options.systemMessage', itemIndex, ''),
				);
				if (systemMessage) {
					if (this.getNodeParameter('local', itemIndex, false) === true) {
						throw new NodeOperationError(
							this.getNode(),
							'System Message is only supported in OpenClaw Gateway mode. Disable Run Locally to use it.',
							{ itemIndex },
						);
					}

					gatewayParams.extraSystemPrompt = systemMessage;
					const rpcTimeoutMs = getWatchdogTimeoutMs(timeoutSeconds);
					args = getGatewayCallArgs(gatewayParams, rpcTimeoutMs);
					processTimeoutMs = rpcTimeoutMs + 10_000;
				}

				const result = await runOpenClawCli({
					binaryPath,
					args,
					cwd: workingDirectory,
					timeoutMs: processTimeoutMs ?? getWatchdogTimeoutMs(timeoutSeconds),
					abortSignal: this.getExecutionCancelSignal(),
				});

				if (result.exitCode !== 0) {
					const messageFromStderr = result.stderr.trim();
					const messageFromStdout = result.stdout.trim();
					throw new NodeOperationError(
						this.getNode(),
						messageFromStderr ||
							messageFromStdout ||
							`OpenClaw CLI exited with code ${result.exitCode ?? `signal ${result.signal}`}`,
						{ itemIndex },
					);
				}

				const json = parseOpenClawOutput(result.stdout);
				json.command = result.command;
				if (this.getNodeParameter('options.includeRawOutput', itemIndex, false) === true) {
					json.rawOutput = {
						stdout: result.stdout,
						stderr: result.stderr,
					};
				}

				returnData.push({
					json,
					pairedItem: { item: itemIndex },
				});
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: getErrorMessage(error) },
						pairedItem: { item: itemIndex },
					});
					continue;
				}

				throw error;
			}
		}

		return [returnData];
	}
}
