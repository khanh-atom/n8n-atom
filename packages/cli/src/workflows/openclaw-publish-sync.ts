import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { dirname, join } from 'path';
import type { IConnections, IDataObject, INode } from 'n8n-workflow';
import { NodeConnectionTypes, UnexpectedError, jsonParse } from 'n8n-workflow';

const OPENCLAW_AGENT_NODE_TYPE = '@n8n/n8n-nodes-langchain.openClawAgent';
const OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE =
	'@n8n/n8n-nodes-langchain.openClawOpenCodeFreeModel';
const OPENCLAW_NINE_ROUTER_MODEL_NODE_TYPE = '@n8n/n8n-nodes-langchain.lmChat9Router';
const OPENCLAW_CONFIG_PATH_ENV = 'OPENCLAW_CONFIG_PATH';
const OPENCLAW_DEFAULT_AGENT_ID = 'main';
const OPENCLAW_DEFAULT_OPEN_CODE_MODEL = 'opencode/big-pickle';
const OPENCLAW_OPEN_CODE_FREE_PROVIDER = 'opencode';
const OPENCLAW_OPEN_CODE_FREE_ENV_VAR = 'OPENCODE_API_KEY';
const OPENCLAW_OPEN_CODE_FREE_ENV_ALIAS = 'OPENCODE_ZEN_API_KEY';
const OPENCLAW_OPEN_CODE_FREE_PUBLIC_KEY = 'public';
const OPENCLAW_STATE_DIR_ENV = 'OPENCLAW_STATE_DIR';
const OPENCLAW_MAIN_SESSION_KEY_SEGMENT = 'main';
const OPENCLAW_SESSION_STORE_FILE = 'sessions.json';
const OPENCLAW_NINE_ROUTER_PROVIDER = '9router';
const OPENCLAW_NINE_ROUTER_DEFAULT_MODEL = 'auto';
const OPENCLAW_NINE_ROUTER_DEFAULT_BASE_URL = 'http://localhost:20128/api/v1';
const OPENCLAW_NINE_ROUTER_API = 'openai-completions';
const OPENCLAW_NINE_ROUTER_CONTEXT_WINDOW = 128_000;
const OPENCLAW_NINE_ROUTER_MAX_TOKENS = 32_000;

export type OpenClawModelSyncCandidate = {
	agentNodeName: string;
	agentId: string;
	modelId: string;
	source: 'connected-model' | 'agent-parameter';
	modelNodeName?: string;
	modelNodeType?: string;
};

export type OpenClawModelSyncAgentDiagnostic = {
	agentNodeName: string;
	typeVersion: number;
	selectorType: string;
	agentId?: string;
	parameterModel?: string;
	connectedModelNodeName?: string;
	connectedModelNodeType?: string;
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
	openCodeFreeAuth?: OpenClawOpenCodeFreeAuthSyncResult;
	nineRouterProvider?: OpenClawNineRouterProviderSyncResult;
	sessionModel?: OpenClawSessionModelSyncResult;
};

export type OpenClawOpenCodeFreeAuthSyncResult = {
	changed: boolean;
	envVar: string;
	targetPath?: string;
	reason: 'configured-public-env' | 'existing-auth' | 'not-open-code-free';
};

export type OpenClawNineRouterProviderSyncResult = {
	changed: boolean;
	provider: string;
	model?: string;
	baseUrl?: string;
	api: typeof OPENCLAW_NINE_ROUTER_API;
	targetPath?: string;
	existingBaseUrl?: string;
	existingApi?: string;
	allowedModelTargetPath?: string;
	reason: 'updated-provider' | 'already-current' | 'not-nine-router' | 'invalid-model-id';
};

export type OpenClawSessionModelSyncResult = {
	changed: boolean;
	sessionKey: string;
	storePath: string;
	provider?: string;
	model?: string;
	existingProvider?: string;
	existingModel?: string;
	existingProviderOverride?: string;
	existingModelOverride?: string;
	targetPath?: string;
	reason:
		| 'updated-session-model'
		| 'already-current'
		| 'missing-session-store'
		| 'missing-session-entry'
		| 'invalid-session-entry'
		| 'invalid-model-id';
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

function getOpenClawStateDir(configPath: string): string {
	return normalizeOptionalString(process.env[OPENCLAW_STATE_DIR_ENV]) ?? dirname(configPath);
}

function getOpenClawSessionStorePath(configPath: string, agentId: string): string {
	return join(
		getOpenClawStateDir(configPath),
		'agents',
		normalizeOpenClawAgentId(agentId),
		'sessions',
		OPENCLAW_SESSION_STORE_FILE,
	);
}

function getOpenClawMainSessionKey(agentId: string): string {
	return `agent:${normalizeOpenClawAgentId(agentId)}:${OPENCLAW_MAIN_SESSION_KEY_SEGMENT}`;
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

function readOpenClawSessionStore(storePath: string): IDataObject {
	const rawStore = readFileSync(storePath, 'utf8').trim();
	if (!rawStore) {
		return {};
	}
	const parsed = jsonParse<unknown>(rawStore, { acceptJSObject: true, repairJSON: true });
	if (!isObject(parsed)) {
		throw new UnexpectedError(`OpenClaw sessions store is not an object: ${storePath}`);
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

function setConfigValue(target: IDataObject, key: string, value: unknown): boolean {
	if (target[key] === value) {
		return false;
	}
	target[key] = value as IDataObject[string];
	return true;
}

function setDefaultConfigValue(target: IDataObject, key: string, value: unknown): boolean {
	if (target[key] !== undefined) {
		return false;
	}
	target[key] = value as IDataObject[string];
	return true;
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

function getModelProvider(value: string): string | undefined {
	return normalizeOptionalString(value.split('/')[0]);
}

function parseModelRef(modelId: string): { provider: string; model: string } | undefined {
	const [providerPart, ...modelParts] = modelId.split('/');
	const provider = normalizeOptionalString(providerPart)?.toLowerCase();
	const model = normalizeOptionalString(modelParts.join('/'));
	if (!provider || !model) {
		return undefined;
	}
	return { provider, model };
}

function toOpenClawNineRouterModelId(rawModel: unknown): string {
	const model = normalizeOptionalString(rawModel) ?? OPENCLAW_NINE_ROUTER_DEFAULT_MODEL;
	const providerPrefix = `${OPENCLAW_NINE_ROUTER_PROVIDER}/`;
	return model.toLowerCase().startsWith(providerPrefix) ? model : `${providerPrefix}${model}`;
}

function getConnectedOpenClawModelId(modelNode: INode): string | undefined {
	if (modelNode.type === OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE) {
		return normalizeOptionalString(modelNode.parameters.model) ?? OPENCLAW_DEFAULT_OPEN_CODE_MODEL;
	}
	if (modelNode.type === OPENCLAW_NINE_ROUTER_MODEL_NODE_TYPE) {
		return toOpenClawNineRouterModelId(modelNode.parameters.model);
	}
	return undefined;
}

function isOpenCodeFreeModelCandidate(candidate: OpenClawModelSyncCandidate): boolean {
	return (
		candidate.source === 'connected-model' &&
		normalizeLowercaseStringOrEmpty(getModelProvider(candidate.modelId)) ===
			OPENCLAW_OPEN_CODE_FREE_PROVIDER
	);
}

function isNineRouterModelCandidate(candidate: OpenClawModelSyncCandidate): boolean {
	return (
		candidate.source === 'connected-model' &&
		normalizeLowercaseStringOrEmpty(getModelProvider(candidate.modelId)) ===
			OPENCLAW_NINE_ROUTER_PROVIDER
	);
}

function hasConfigEnvValue(config: IDataObject, envVar: string): boolean {
	const env = config.env;
	if (!isObject(env)) {
		return false;
	}
	if (normalizeOptionalString(env[envVar])) {
		return true;
	}
	const vars = env.vars;
	return isObject(vars) && normalizeOptionalString(vars[envVar]) !== undefined;
}

function hasConfiguredOpenCodeAuth(config: IDataObject): boolean {
	if (
		hasConfigEnvValue(config, OPENCLAW_OPEN_CODE_FREE_ENV_VAR) ||
		hasConfigEnvValue(config, OPENCLAW_OPEN_CODE_FREE_ENV_ALIAS)
	) {
		return true;
	}

	const models = config.models;
	if (!isObject(models)) {
		return false;
	}
	const providers = models.providers;
	if (!isObject(providers)) {
		return false;
	}
	const openCodeProvider = providers[OPENCLAW_OPEN_CODE_FREE_PROVIDER];
	if (!isObject(openCodeProvider)) {
		return false;
	}
	return 'apiKey' in openCodeProvider;
}

function syncOpenCodeFreePublicAuth(
	config: IDataObject,
	candidate: OpenClawModelSyncCandidate,
): OpenClawOpenCodeFreeAuthSyncResult {
	if (!isOpenCodeFreeModelCandidate(candidate)) {
		return {
			changed: false,
			envVar: OPENCLAW_OPEN_CODE_FREE_ENV_VAR,
			reason: 'not-open-code-free',
		};
	}

	if (hasConfiguredOpenCodeAuth(config)) {
		return {
			changed: false,
			envVar: OPENCLAW_OPEN_CODE_FREE_ENV_VAR,
			reason: 'existing-auth',
		};
	}

	const env = ensureDataObject(config, 'env');
	const vars = ensureDataObject(env, 'vars');
	vars[OPENCLAW_OPEN_CODE_FREE_ENV_VAR] = OPENCLAW_OPEN_CODE_FREE_PUBLIC_KEY;
	return {
		changed: true,
		envVar: OPENCLAW_OPEN_CODE_FREE_ENV_VAR,
		targetPath: `env.vars.${OPENCLAW_OPEN_CODE_FREE_ENV_VAR}`,
		reason: 'configured-public-env',
	};
}

function getNineRouterModelDefinition(model: string): IDataObject {
	return {
		id: model,
		name: model === OPENCLAW_NINE_ROUTER_DEFAULT_MODEL ? '9Router Auto' : model,
		api: OPENCLAW_NINE_ROUTER_API,
		reasoning: false,
		input: ['text', 'image'],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: OPENCLAW_NINE_ROUTER_CONTEXT_WINDOW,
		maxTokens: OPENCLAW_NINE_ROUTER_MAX_TOKENS,
	};
}

function syncNineRouterProviderConfig(
	config: IDataObject,
	candidate: OpenClawModelSyncCandidate,
): OpenClawNineRouterProviderSyncResult {
	if (!isNineRouterModelCandidate(candidate)) {
		return {
			changed: false,
			provider: OPENCLAW_NINE_ROUTER_PROVIDER,
			api: OPENCLAW_NINE_ROUTER_API,
			reason: 'not-nine-router',
		};
	}

	const modelRef = parseModelRef(candidate.modelId);
	if (!modelRef || modelRef.provider !== OPENCLAW_NINE_ROUTER_PROVIDER) {
		return {
			changed: false,
			provider: OPENCLAW_NINE_ROUTER_PROVIDER,
			api: OPENCLAW_NINE_ROUTER_API,
			reason: 'invalid-model-id',
		};
	}

	const models = ensureDataObject(config, 'models');
	const providers = ensureDataObject(models, 'providers');
	const provider = ensureDataObject(providers, OPENCLAW_NINE_ROUTER_PROVIDER);
	const existingBaseUrl = normalizeOptionalString(provider.baseUrl);
	const existingApi = normalizeOptionalString(provider.api);

	let changed = false;
	changed =
		setDefaultConfigValue(provider, 'baseUrl', OPENCLAW_NINE_ROUTER_DEFAULT_BASE_URL) || changed;
	changed = setConfigValue(provider, 'api', OPENCLAW_NINE_ROUTER_API) || changed;

	let providerModels: IDataObject[];
	if (Array.isArray(provider.models)) {
		providerModels = provider.models.filter(isObject) as IDataObject[];
		if (providerModels.length !== provider.models.length) {
			provider.models = providerModels;
			changed = true;
		}
	} else {
		providerModels = [];
		provider.models = providerModels;
		changed = true;
	}

	const existingModel = providerModels.find(
		(model) => normalizeOptionalString(model.id) === modelRef.model,
	);

	if (existingModel) {
		changed = setConfigValue(existingModel, 'api', OPENCLAW_NINE_ROUTER_API) || changed;
		changed = setDefaultConfigValue(existingModel, 'name', modelRef.model) || changed;
		changed = setDefaultConfigValue(existingModel, 'reasoning', false) || changed;
		changed = setDefaultConfigValue(existingModel, 'input', ['text', 'image']) || changed;
		changed =
			setDefaultConfigValue(existingModel, 'cost', {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
			}) || changed;
		changed =
			setDefaultConfigValue(existingModel, 'contextWindow', OPENCLAW_NINE_ROUTER_CONTEXT_WINDOW) ||
			changed;
		changed =
			setDefaultConfigValue(existingModel, 'maxTokens', OPENCLAW_NINE_ROUTER_MAX_TOKENS) || changed;
	} else {
		providerModels.push(getNineRouterModelDefinition(modelRef.model));
		changed = true;
	}

	const agents = ensureDataObject(config, 'agents');
	const defaults = ensureDataObject(agents, 'defaults');
	const defaultModels = ensureDataObject(defaults, 'models');
	const fullModelRef = `${modelRef.provider}/${modelRef.model}`;
	if (!isObject(defaultModels[fullModelRef])) {
		defaultModels[fullModelRef] = {};
		changed = true;
	}

	return {
		changed,
		provider: OPENCLAW_NINE_ROUTER_PROVIDER,
		model: modelRef.model,
		baseUrl: normalizeOptionalString(provider.baseUrl),
		api: OPENCLAW_NINE_ROUTER_API,
		targetPath: `models.providers.${OPENCLAW_NINE_ROUTER_PROVIDER}`,
		existingBaseUrl,
		existingApi,
		allowedModelTargetPath: `agents.defaults.models.${fullModelRef}`,
		reason: changed ? 'updated-provider' : 'already-current',
	};
}

function applySessionModelOverride(
	entry: Record<string, unknown>,
	modelRef: { provider: string; model: string },
): boolean {
	let changed = false;
	let selectionUpdated = false;

	if (entry.providerOverride !== modelRef.provider) {
		entry.providerOverride = modelRef.provider;
		changed = true;
		selectionUpdated = true;
	}
	if (entry.modelOverride !== modelRef.model) {
		entry.modelOverride = modelRef.model;
		changed = true;
		selectionUpdated = true;
	}
	if (entry.modelOverrideSource !== 'auto') {
		entry.modelOverrideSource = 'auto';
		changed = true;
	}

	const runtimeProvider = normalizeOptionalString(entry.modelProvider);
	const runtimeModel = normalizeOptionalString(entry.model);
	const runtimePresent = runtimeProvider !== undefined || runtimeModel !== undefined;
	const runtimeAligned = runtimeProvider === modelRef.provider && runtimeModel === modelRef.model;
	if (runtimePresent && (selectionUpdated || !runtimeAligned)) {
		if (entry.modelProvider !== undefined) {
			delete entry.modelProvider;
			changed = true;
		}
		if (entry.model !== undefined) {
			delete entry.model;
			changed = true;
		}
	}

	if (
		entry.contextTokens !== undefined &&
		(selectionUpdated || (runtimePresent && !runtimeAligned))
	) {
		delete entry.contextTokens;
		changed = true;
	}

	if (changed) {
		delete entry.fallbackNoticeSelectedModel;
		delete entry.fallbackNoticeActiveModel;
		delete entry.fallbackNoticeReason;
		entry.updatedAt = Date.now();
	}

	return changed;
}

function syncOpenClawSessionModel(
	configPath: string,
	candidate: OpenClawModelSyncCandidate,
): OpenClawSessionModelSyncResult {
	const sessionKey = getOpenClawMainSessionKey(candidate.agentId);
	const storePath = getOpenClawSessionStorePath(configPath, candidate.agentId);
	const modelRef = parseModelRef(candidate.modelId);
	if (!modelRef) {
		return {
			changed: false,
			sessionKey,
			storePath,
			reason: 'invalid-model-id',
		};
	}

	if (!existsSync(storePath)) {
		return {
			changed: false,
			sessionKey,
			storePath,
			provider: modelRef.provider,
			model: modelRef.model,
			reason: 'missing-session-store',
		};
	}

	const store = readOpenClawSessionStore(storePath);
	const entry = store[sessionKey];
	if (entry === undefined) {
		return {
			changed: false,
			sessionKey,
			storePath,
			provider: modelRef.provider,
			model: modelRef.model,
			reason: 'missing-session-entry',
		};
	}
	if (!isObject(entry)) {
		return {
			changed: false,
			sessionKey,
			storePath,
			provider: modelRef.provider,
			model: modelRef.model,
			reason: 'invalid-session-entry',
		};
	}

	const existingProvider = normalizeOptionalString(entry.modelProvider);
	const existingModel = normalizeOptionalString(entry.model);
	const existingProviderOverride = normalizeOptionalString(entry.providerOverride);
	const existingModelOverride = normalizeOptionalString(entry.modelOverride);
	const changed = applySessionModelOverride(entry, modelRef);
	if (changed) {
		mkdirSync(dirname(storePath), { recursive: true });
		writeFileSync(storePath, `${JSON.stringify(store, null, 2)}\n`, 'utf8');
	}

	return {
		changed,
		sessionKey,
		storePath,
		provider: modelRef.provider,
		model: modelRef.model,
		existingProvider,
		existingModel,
		existingProviderOverride,
		existingModelOverride,
		targetPath: `${sessionKey}.providerOverride/modelOverride`,
		reason: changed ? 'updated-session-model' : 'already-current',
	};
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

function isSupportedOpenClawModelNode(node: INode | undefined): boolean {
	return (
		node?.type === OPENCLAW_OPEN_CODE_FREE_MODEL_NODE_TYPE ||
		node?.type === OPENCLAW_NINE_ROUTER_MODEL_NODE_TYPE
	);
}

function findConnectedOpenClawModelNode(
	agentNode: INode,
	nodesByName: Map<string, INode>,
	connections: IConnections,
): INode | undefined {
	for (const [sourceNodeName, sourceConnections] of Object.entries(connections)) {
		const sourceNode = nodesByName.get(sourceNodeName);
		if (!isSupportedOpenClawModelNode(sourceNode)) {
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
						isSupportedOpenClawModelNode(sourceNode) &&
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
	const modelNodeCount = nodes.filter((node) => isSupportedOpenClawModelNode(node)).length;
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
			? getConnectedOpenClawModelId(connectedModelNode)
			: undefined;
		const parameterModel = normalizeOptionalString(agentNode.parameters.model);
		const modelId = connectedModel ?? parameterModel;

		if (!modelId) {
			agentDiagnostics.push({
				...diagnosticBase,
				parameterModel,
				connectedModelNodeName: connectedModelNode?.name,
				connectedModelNodeType: connectedModelNode?.type,
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
			modelNodeType: connectedModelNode?.type,
		});
		agentDiagnostics.push({
			...diagnosticBase,
			parameterModel,
			connectedModelNodeName: connectedModelNode?.name,
			connectedModelNodeType: connectedModelNode?.type,
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
	let configChanged = false;
	let changed = false;

	for (const candidate of candidates) {
		const { target, targetPath } = getOpenClawAgentConfigTarget(config, candidate.agentId);
		const existingModel = getModelPrimary(target.model);
		const candidateChanged = setModelPrimary(target, 'model', candidate.modelId);
		const openCodeFreeAuth = syncOpenCodeFreePublicAuth(config, candidate);
		const nineRouterProvider = syncNineRouterProviderConfig(config, candidate);
		const sessionModel = syncOpenClawSessionModel(configPath, candidate);
		configChanged =
			candidateChanged || openCodeFreeAuth.changed || nineRouterProvider.changed || configChanged;
		changed =
			candidateChanged ||
			openCodeFreeAuth.changed ||
			nineRouterProvider.changed ||
			sessionModel.changed ||
			changed;
		results.push({
			...candidate,
			changed: candidateChanged,
			existingModel,
			targetPath,
			openCodeFreeAuth,
			nineRouterProvider,
			sessionModel,
		});
	}

	if (configChanged) {
		mkdirSync(dirname(configPath), { recursive: true });
		writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
	}

	return { changed, configPath, results };
}
