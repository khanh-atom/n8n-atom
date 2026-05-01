import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { dirname, join } from 'path';
import type { IConnections, IDataObject, INode } from 'n8n-workflow';
import { NodeConnectionTypes, UnexpectedError, jsonParse } from 'n8n-workflow';

const OPENCLAW_AGENT_NODE_TYPE = '@n8n/n8n-nodes-langchain.openClawAgent';
const OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE =
	'@n8n/n8n-nodes-langchain.openClawOpenCodeFreeModel';
const OPENCLAW_CONFIG_PATH_ENV = 'OPENCLAW_CONFIG_PATH';
const OPENCLAW_DEFAULT_AGENT_ID = 'main';
const OPENCLAW_DEFAULT_OPEN_CODE_MODEL = 'opencode/big-pickle';

export type OpenClawModelSyncCandidate = {
	agentNodeName: string;
	agentId: string;
	modelId: string;
	source: 'connected-model' | 'agent-parameter';
	modelNodeName?: string;
};

export type OpenClawModelSyncAgentDiagnostic = {
	agentNodeName: string;
	typeVersion: number;
	selectorType: string;
	agentId?: string;
	parameterModel?: string;
	connectedModelNodeName?: string;
	connectedModelId?: string;
	resolvedModelId?: string;
	status: 'candidate' | 'skipped';
	reason?: 'unsupported-version' | 'route-not-agent' | 'missing-model';
};

export type OpenClawModelSyncConnectionDiagnostic = {
	sourceNodeName: string;
	sourceNodeType?: string;
	targetNodeName: string;
	targetNodeType?: string;
	connectionType: string;
	outputIndex: number;
	inputIndex: number;
};

export type OpenClawModelSyncInspection = {
	candidates: OpenClawModelSyncCandidate[];
	agentNodeCount: number;
	modelNodeCount: number;
	connectionCount: number;
	agentDiagnostics: OpenClawModelSyncAgentDiagnostic[];
	modelConnections: OpenClawModelSyncConnectionDiagnostic[];
};

export type OpenClawModelSyncResult = OpenClawModelSyncCandidate & {
	changed: boolean;
	existingModel?: string;
	targetPath: string;
};

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeOptionalString(value: unknown): string | undefined {
	if (typeof value !== 'string') {
		return undefined;
	}
	const trimmed = value.trim();
	return trimmed || undefined;
}

function normalizeLowercaseStringOrEmpty(value: string | undefined | null): string {
	return (value ?? '').trim().toLowerCase();
}

function normalizeOpenClawAgentId(value: unknown): string {
	const trimmed = typeof value === 'string' ? value.trim() : '';
	if (!trimmed) {
		return OPENCLAW_DEFAULT_AGENT_ID;
	}
	const normalized = normalizeLowercaseStringOrEmpty(trimmed);
	if (/^[a-z0-9][a-z0-9_-]{0,63}$/i.test(trimmed)) {
		return normalized;
	}
	return (
		normalized
			.replace(/[^a-z0-9_-]+/g, '-')
			.replace(/^-+/, '')
			.replace(/-+$/, '')
			.slice(0, 64) || OPENCLAW_DEFAULT_AGENT_ID
	);
}

function getOpenClawConfigPath(): string | undefined {
	const configuredPath = normalizeOptionalString(process.env[OPENCLAW_CONFIG_PATH_ENV]);
	if (configuredPath) {
		return configuredPath;
	}
	const home =
		normalizeOptionalString(process.env.HOME) ?? normalizeOptionalString(process.env.USERPROFILE);
	return home ? join(home, '.openclaw', 'openclaw.json') : undefined;
}

function readOpenClawConfig(configPath: string): IDataObject {
	if (!existsSync(configPath)) {
		return {};
	}
	const rawConfig = readFileSync(configPath, 'utf8').trim();
	if (!rawConfig) {
		return {};
	}
	const parsed = jsonParse<unknown>(rawConfig, { acceptJSObject: true, repairJSON: true });
	if (!isObject(parsed)) {
		throw new UnexpectedError(`OpenClaw config is not an object: ${configPath}`);
	}
	return parsed as IDataObject;
}

function ensureDataObject(parent: IDataObject, key: string): IDataObject {
	const existing = parent[key];
	if (isObject(existing)) {
		return existing as IDataObject;
	}
	const next: IDataObject = {};
	parent[key] = next;
	return next;
}

function getModelPrimary(value: unknown): string | undefined {
	if (typeof value === 'string') {
		return normalizeOptionalString(value);
	}
	if (isObject(value)) {
		return normalizeOptionalString(value.primary);
	}
	return undefined;
}

function setModelPrimary(target: IDataObject, key: string, modelId: string): boolean {
	const existing = target[key];
	if (getModelPrimary(existing) === modelId) {
		return false;
	}
	if (isObject(existing)) {
		target[key] = { ...existing, primary: modelId };
	} else {
		target[key] = modelId;
	}
	return true;
}

function getOpenClawAgentConfigTarget(
	config: IDataObject,
	agentId: string,
): { target: IDataObject; targetPath: string } {
	const agents = ensureDataObject(config, 'agents');
	const existingList = agents.list;

	if (Array.isArray(existingList)) {
		let normalizedList = existingList.filter(isObject) as IDataObject[];
		if (normalizedList.length !== existingList.length) {
			agents.list = normalizedList;
		}

		let entry = normalizedList.find(
			(item) => normalizeOpenClawAgentId(item.id) === normalizeOpenClawAgentId(agentId),
		);
		if (!entry) {
			entry = { id: agentId };
			normalizedList = [...normalizedList, entry];
			agents.list = normalizedList;
		}
		return { target: entry, targetPath: `agents.list[id=${agentId}].model` };
	}

	if (agentId === OPENCLAW_DEFAULT_AGENT_ID) {
		return {
			target: ensureDataObject(agents, 'defaults'),
			targetPath: 'agents.defaults.model',
		};
	}

	const list = [{ id: OPENCLAW_DEFAULT_AGENT_ID, default: true }, { id: agentId }];
	agents.list = list;
	return { target: list[1], targetPath: `agents.list[id=${agentId}].model` };
}

function findConnectedOpenClawModelNode(
	agentNode: INode,
	nodesByName: Map<string, INode>,
	connections: IConnections,
): INode | undefined {
	for (const [sourceNodeName, sourceConnections] of Object.entries(connections)) {
		const sourceNode = nodesByName.get(sourceNodeName);
		if (sourceNode?.type !== OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE) {
			continue;
		}

		const languageModelConnections = sourceConnections[NodeConnectionTypes.AiLanguageModel];
		if (!languageModelConnections) {
			continue;
		}

		for (const outputConnections of languageModelConnections) {
			if (
				outputConnections?.some(
					(connection) =>
						connection.node === agentNode.name &&
						connection.type === NodeConnectionTypes.AiLanguageModel,
				)
			) {
				return sourceNode;
			}
		}
	}
	return undefined;
}

function inspectOpenClawModelConnections(
	nodesByName: Map<string, INode>,
	connections: IConnections,
): Pick<OpenClawModelSyncInspection, 'connectionCount' | 'modelConnections'> {
	let connectionCount = 0;
	const modelConnections: OpenClawModelSyncConnectionDiagnostic[] = [];

	for (const [sourceNodeName, sourceConnections] of Object.entries(connections)) {
		const sourceNode = nodesByName.get(sourceNodeName);
		for (const [connectionType, outputConnectionGroups] of Object.entries(sourceConnections)) {
			outputConnectionGroups.forEach((outputConnections, outputIndex) => {
				if (!outputConnections) {
					return;
				}
				for (const connection of outputConnections) {
					connectionCount++;
					const targetNode = nodesByName.get(connection.node);
					const isOpenClawModelConnection =
						sourceNode?.type === OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE &&
						targetNode?.type === OPENCLAW_AGENT_NODE_TYPE &&
						connection.type === NodeConnectionTypes.AiLanguageModel;

					if (isOpenClawModelConnection) {
						modelConnections.push({
							sourceNodeName,
							sourceNodeType: sourceNode?.type,
							targetNodeName: connection.node,
							targetNodeType: targetNode?.type,
							connectionType,
							outputIndex,
							inputIndex: connection.index,
						});
					}
				}
			});
		}
	}

	return { connectionCount, modelConnections };
}

export function inspectOpenClawModelSyncCandidates(
	nodes: INode[],
	connections: IConnections,
): OpenClawModelSyncInspection {
	const nodesByName = new Map(nodes.map((node) => [node.name, node]));
	const candidates: OpenClawModelSyncCandidate[] = [];
	const agentDiagnostics: OpenClawModelSyncAgentDiagnostic[] = [];
	const agentNodeCount = nodes.filter((node) => node.type === OPENCLAW_AGENT_NODE_TYPE).length;
	const modelNodeCount = nodes.filter(
		(node) => node.type === OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE,
	).length;
	const connectionInspection = inspectOpenClawModelConnections(nodesByName, connections);

	for (const agentNode of nodes) {
		if (agentNode.type !== OPENCLAW_AGENT_NODE_TYPE) {
			continue;
		}

		const selectorType = normalizeOptionalString(agentNode.parameters.selectorType) ?? 'agent';
		const agentId = normalizeOpenClawAgentId(agentNode.parameters.agentId);
		const diagnosticBase = {
			agentNodeName: agentNode.name,
			typeVersion: agentNode.typeVersion,
			selectorType,
			agentId,
		};

		if (agentNode.typeVersion < 2) {
			agentDiagnostics.push({
				...diagnosticBase,
				status: 'skipped',
				reason: 'unsupported-version',
			});
			continue;
		}

		if (selectorType !== 'agent') {
			agentDiagnostics.push({
				...diagnosticBase,
				status: 'skipped',
				reason: 'route-not-agent',
			});
			continue;
		}

		const connectedModelNode = findConnectedOpenClawModelNode(agentNode, nodesByName, connections);
		const connectedModel = connectedModelNode
			? (normalizeOptionalString(connectedModelNode.parameters.model) ??
				OPENCLAW_DEFAULT_OPEN_CODE_MODEL)
			: undefined;
		const parameterModel = normalizeOptionalString(agentNode.parameters.model);
		const modelId = connectedModel ?? parameterModel;

		if (!modelId) {
			agentDiagnostics.push({
				...diagnosticBase,
				parameterModel,
				connectedModelNodeName: connectedModelNode?.name,
				connectedModelId: connectedModel,
				status: 'skipped',
				reason: 'missing-model',
			});
			continue;
		}

		candidates.push({
			agentNodeName: agentNode.name,
			agentId,
			modelId,
			source: connectedModelNode ? 'connected-model' : 'agent-parameter',
			modelNodeName: connectedModelNode?.name,
		});
		agentDiagnostics.push({
			...diagnosticBase,
			parameterModel,
			connectedModelNodeName: connectedModelNode?.name,
			connectedModelId: connectedModel,
			resolvedModelId: modelId,
			status: 'candidate',
		});
	}

	return {
		candidates,
		agentNodeCount,
		modelNodeCount,
		agentDiagnostics,
		connectionCount: connectionInspection.connectionCount,
		modelConnections: connectionInspection.modelConnections,
	};
}

export function resolveOpenClawModelSyncCandidates(
	nodes: INode[],
	connections: IConnections,
): OpenClawModelSyncCandidate[] {
	return inspectOpenClawModelSyncCandidates(nodes, connections).candidates;
}

export function syncOpenClawModelConfigCandidates(candidates: OpenClawModelSyncCandidate[]): {
	changed: boolean;
	configPath: string;
	results: OpenClawModelSyncResult[];
} {
	const configPath = getOpenClawConfigPath();
	if (!configPath) {
		throw new UnexpectedError(
			`Could not determine OpenClaw config path. Set ${OPENCLAW_CONFIG_PATH_ENV} or HOME for the n8n process.`,
		);
	}

	const config = readOpenClawConfig(configPath);
	const results: OpenClawModelSyncResult[] = [];
	let changed = false;

	for (const candidate of candidates) {
		const { target, targetPath } = getOpenClawAgentConfigTarget(config, candidate.agentId);
		const existingModel = getModelPrimary(target.model);
		const candidateChanged = setModelPrimary(target, 'model', candidate.modelId);
		changed = candidateChanged || changed;
		results.push({ ...candidate, changed: candidateChanged, existingModel, targetPath });
	}

	if (changed) {
		mkdirSync(dirname(configPath), { recursive: true });
		writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
	}

	return { changed, configPath, results };
}
