import { execSync, spawn, type ChildProcess } from 'child_process';
import { randomUUID } from 'crypto';
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'fs';
import { delimiter, dirname, join } from 'path';

import {
	ApplicationError,
	NodeConnectionTypes,
	NodeOperationError,
	jsonParse,
	type IDataObject,
	type IExecuteFunctions,
	type INodeExecutionData,
	type INodeType,
	type INodeTypeBaseDescription,
	type INodeTypeDescription,
	type ITriggerFunctions,
	type ITriggerResponse,
} from 'n8n-workflow';

export interface ChannelConfig {
	channelType: string;
	botToken?: string;
	accountId?: string;
	dmPolicy?: string;
	groupPolicy?: string;
	allowFrom?: string[];
	phoneNumberId?: string;
	accessToken?: string;
	extra?: IDataObject;
}

/**
 * Configuration returned by Model sub-nodes connected to the OpenClaw agent.
 * The modelId is used to override the --model CLI flag.
 */
export interface ModelConfig {
	modelId: string;
	modelSource: string;
	extra?: IDataObject;
}

/**
 * Configuration returned by Plugin sub-nodes connected to the OpenClaw agent.
 *
 * - **local**: scans `pluginPath` for `openclaw.plugin.json` and loads manifest info.
 * - **cloud**: references a ClawHub plugin by `pluginId` (e.g. "clawhub:openai").
 */
export interface PluginConfig {
	pluginSource: 'local' | 'cloud';
	/** Directory path for local plugins (scanned for openclaw.plugin.json). */
	pluginPath?: string;
	/** Plugin manifest data loaded from openclaw.plugin.json (local source). */
	pluginManifest?: {
		id?: string;
		name?: string;
		description?: string;
		version?: string;
		providers?: string[];
		channels?: string[];
	};
	/** ClawHub plugin package name (cloud source), e.g. "openai" or "@scope/pkg". */
	pluginId?: string;
	/** ClawHub plugin version (cloud source). Leave empty for latest. */
	pluginVersion?: string;
	extra?: IDataObject;
}

export interface McpServerConfig {
	mcpServerSource: 'openclaw';
	serverName: string;
	url: string;
	transport?: 'sse' | 'streamable-http';
	headers?: Record<string, string | number | boolean>;
	connectionTimeoutMs?: number;
	extra?: IDataObject;
}

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
const OPENCLAW_CONFIG_PATH_ENV = 'OPENCLAW_CONFIG_PATH';
const OPENCLAW_DEFAULT_ACCOUNT_ID = 'default';
const OPEN_CODE_FREE_MODEL_SOURCE = 'opencode-free';
const OPEN_CODE_FREE_PROVIDER = 'opencode';
const OPEN_CODE_FREE_ENV_VAR = 'OPENCODE_API_KEY';
const OPEN_CODE_FREE_ENV_ALIAS = 'OPENCODE_ZEN_API_KEY';
const OPEN_CODE_FREE_PUBLIC_KEY = 'public';
const NINE_ROUTER_MODEL_SOURCE = '9router';
const NINE_ROUTER_PROVIDER = '9router';
const NINE_ROUTER_DEFAULT_BASE_URL = 'http://localhost:20128/api/v1';
const NINE_ROUTER_API = 'openai-completions';
const NINE_ROUTER_CONTEXT_WINDOW = 128_000;
const NINE_ROUTER_MAX_TOKENS = 32_000;

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

function normalizeLowercaseStringOrEmpty(value: string | undefined): string {
	return (value ?? '').trim().toLowerCase();
}

function getObjectKeys(value: unknown): string[] | undefined {
	if (!isObject(value)) {
		return undefined;
	}
	return Object.keys(value);
}

function getModelDataPreview(value: unknown): string | undefined {
	if (value === undefined || value === null) {
		return undefined;
	}
	if (typeof value === 'string') {
		return value.length <= 300 ? value : `${value.slice(0, 300)}...`;
	}
	if (typeof value !== 'object') {
		return String(value);
	}
	if (Array.isArray(value)) {
		return `[array length=${value.length}]`;
	}
	if (isObject(value)) {
		const modelId = normalizeOptionalString(value.modelId);
		const modelSource = normalizeOptionalString(value.modelSource);
		const extra = isObject(value.extra) ? value.extra : undefined;
		if (modelId || modelSource || extra) {
			return JSON.stringify({
				modelId,
				modelSource,
				extraKeys: getObjectKeys(extra),
			});
		}
		return `[object keys=${Object.keys(value).slice(0, 20).join(',')}]`;
	}
	return undefined;
}

function getModelProvider(modelId: string | undefined): string | undefined {
	const provider = modelId?.split('/')[0];
	return normalizeOptionalString(provider);
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

function isOpenCodeFreeModel(
	modelConfig: ModelConfig | undefined,
	modelId: string | undefined,
): boolean {
	return (
		modelConfig?.modelSource === OPEN_CODE_FREE_MODEL_SOURCE &&
		normalizeLowercaseStringOrEmpty(getModelProvider(modelId)) === OPEN_CODE_FREE_PROVIDER
	);
}

function hasOpenCodeAuthEnv(env: NodeJS.ProcessEnv): boolean {
	return (
		normalizeOptionalString(env[OPEN_CODE_FREE_ENV_VAR]) !== undefined ||
		normalizeOptionalString(env[OPEN_CODE_FREE_ENV_ALIAS]) !== undefined
	);
}

function applyOpenCodeFreeAuthEnv(params: {
	env: NodeJS.ProcessEnv;
	modelConfig: ModelConfig | undefined;
	modelId: string | undefined;
}): { applied: boolean; envVar: string; reason: string } {
	if (!isOpenCodeFreeModel(params.modelConfig, params.modelId)) {
		return {
			applied: false,
			envVar: OPEN_CODE_FREE_ENV_VAR,
			reason: 'not-open-code-free',
		};
	}

	if (hasOpenCodeAuthEnv(params.env) || hasOpenCodeAuthEnv(process.env)) {
		return {
			applied: false,
			envVar: OPEN_CODE_FREE_ENV_VAR,
			reason: 'existing-auth-env',
		};
	}

	params.env[OPEN_CODE_FREE_ENV_VAR] = OPEN_CODE_FREE_PUBLIC_KEY;
	return {
		applied: true,
		envVar: OPEN_CODE_FREE_ENV_VAR,
		reason: 'configured-public-env',
	};
}

function getOpenClawConfigPath(): string | undefined {
	const configuredPath = normalizeOptionalString(process.env[OPENCLAW_CONFIG_PATH_ENV]);
	if (configuredPath) {
		return configuredPath;
	}
	const home = getHomeDirectory();
	return home ? join(home, '.openclaw', 'openclaw.json') : undefined;
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

function deleteConfigValue(target: IDataObject, key: string): boolean {
	if (target[key] === undefined) {
		return false;
	}
	delete target[key];
	return true;
}

function setOptionalConfigValue(target: IDataObject, key: string, value: unknown): boolean {
	if (value === undefined) {
		return deleteConfigValue(target, key);
	}
	return setConfigValue(target, key, value);
}

function setOptionalStringArrayConfigValue(
	target: IDataObject,
	key: string,
	value: string[] | undefined,
): boolean {
	if (value === undefined) {
		return deleteConfigValue(target, key);
	}
	if (value.length === 0) {
		return deleteConfigValue(target, key);
	}
	const existing = target[key];
	if (
		Array.isArray(existing) &&
		existing.length === value.length &&
		existing.every((entry, index) => entry === value[index])
	) {
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

function normalizeOpenClawAccountId(value: string | undefined): string | undefined {
	const trimmed = normalizeOptionalString(value)?.toLowerCase();
	if (!trimmed) {
		return undefined;
	}
	const normalized = trimmed
		.replace(/[^a-z0-9_-]+/g, '-')
		.replace(/^-+/, '')
		.replace(/-+$/, '')
		.slice(0, 64);
	if (!normalized || ['__proto__', 'constructor', 'prototype'].includes(normalized)) {
		return undefined;
	}
	return normalized;
}

function normalizeOpenClawMcpServerName(value: string | undefined): string | undefined {
	const trimmed = normalizeOptionalString(value);
	if (!trimmed) {
		return undefined;
	}
	const normalized = trimmed
		.replace(/[^A-Za-z0-9_-]+/g, '-')
		.replace(/^-+/, '')
		.replace(/-+$/, '')
		.slice(0, 80);
	if (!normalized || ['__proto__', 'constructor', 'prototype'].includes(normalized)) {
		return undefined;
	}
	return normalized;
}

function isHttpUrl(value: string): boolean {
	try {
		const parsed = new URL(value);
		return parsed.protocol === 'http:' || parsed.protocol === 'https:';
	} catch {
		return false;
	}
}

function normalizeMcpTransport(value: unknown): McpServerConfig['transport'] | undefined {
	if (value === 'sse' || value === 'streamable-http') {
		return value;
	}
	if (value === 'http') {
		return 'streamable-http';
	}
	return undefined;
}

function normalizeMcpHeaders(
	value: unknown,
): Record<string, string | number | boolean> | undefined {
	let rawHeaders: unknown = value;
	if (typeof value === 'string') {
		const trimmed = value.trim();
		if (!trimmed || trimmed === '{}') {
			return undefined;
		}
		rawHeaders = jsonParse<unknown>(trimmed, { acceptJSObject: true, repairJSON: true });
	}
	if (!isObject(rawHeaders)) {
		return undefined;
	}
	const headers = Object.fromEntries(
		Object.entries(rawHeaders).filter(
			(entry): entry is [string, string | number | boolean] =>
				typeof entry[1] === 'string' ||
				typeof entry[1] === 'number' ||
				typeof entry[1] === 'boolean',
		),
	);
	return Object.keys(headers).length > 0 ? headers : undefined;
}

function normalizePositiveInteger(value: unknown): number | undefined {
	if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
		return undefined;
	}
	return Math.floor(value);
}

function getMcpServerConfigFromParameters(parameters: Record<string, unknown>): {
	config?: McpServerConfig;
	reason?: string;
} {
	const serverName = normalizeOptionalString(parameters.serverName);
	const url = normalizeOptionalString(parameters.endpointUrl);
	if (!serverName && !url) {
		return { reason: 'not-mcp-server-node' };
	}
	if (!serverName) {
		return { reason: 'missing-server-name' };
	}
	if (!url) {
		return { reason: 'missing-endpoint-url' };
	}

	const options = isObject(parameters.options) ? parameters.options : {};
	let headers: McpServerConfig['headers'];
	try {
		headers = normalizeMcpHeaders(options.headers);
	} catch (error) {
		return { reason: `invalid-headers:${getErrorMessage(error)}` };
	}

	return {
		config: {
			mcpServerSource: 'openclaw',
			serverName,
			url,
			transport: normalizeMcpTransport(parameters.transport),
			headers,
			connectionTimeoutMs: normalizePositiveInteger(options.connectionTimeoutMs),
		},
	};
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
		throw new ApplicationError(`OpenClaw config is not an object: ${configPath}`);
	}
	return parsed as IDataObject;
}

function syncChannelConfig(params: {
	channelType: string;
	botToken?: string;
	replyAccount?: string;
	dmPolicy?: string;
	groupPolicy?: string;
	allowFrom?: string[];
}): { accountId?: string; changed: boolean; configPath: string; targetPath: string } {
	const configPath = getOpenClawConfigPath();
	if (!configPath) {
		throw new ApplicationError(
			`Could not determine OpenClaw config path. Set ${OPENCLAW_CONFIG_PATH_ENV} or HOME for the n8n process.`,
		);
	}
	const config = readOpenClawConfig(configPath);
	const channels = ensureDataObject(config, 'channels');
	const channel = ensureDataObject(channels, params.channelType);
	const accountId = normalizeOpenClawAccountId(params.replyAccount);
	const useAccountTarget = !!accountId && accountId !== OPENCLAW_DEFAULT_ACCOUNT_ID;
	const target = useAccountTarget
		? ensureDataObject(ensureDataObject(channel, 'accounts'), accountId)
		: channel;
	const targetPath = useAccountTarget
		? `channels.${params.channelType}.accounts.${accountId}`
		: `channels.${params.channelType}`;
	const dmPolicy = normalizeOptionalString(params.dmPolicy);
	const groupPolicy = normalizeOptionalString(params.groupPolicy);

	console.log('[OpenClawAgentV2] syncChannelConfig: start', {
		channelType: params.channelType,
		accountId: accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
		targetPath,
		configPath,
		hasBotToken: !!params.botToken,
		dmPolicy: dmPolicy ?? '(openclaw-default)',
		groupPolicy: groupPolicy ?? '(openclaw-default)',
		allowFromCount: params.allowFrom?.length,
	});

	let changed = false;
	changed = setConfigValue(channel, 'enabled', true) || changed;
	if (useAccountTarget) {
		changed = setConfigValue(target, 'enabled', true) || changed;
	}
	changed =
		(dmPolicy
			? setConfigValue(target, 'dmPolicy', dmPolicy)
			: setDefaultConfigValue(target, 'dmPolicy', 'pairing')) || changed;
	changed =
		(groupPolicy
			? setConfigValue(target, 'groupPolicy', groupPolicy)
			: setDefaultConfigValue(target, 'groupPolicy', 'allowlist')) || changed;
	if (params.allowFrom !== undefined) {
		changed = setOptionalStringArrayConfigValue(target, 'allowFrom', params.allowFrom) || changed;
	}

	if (params.botToken) {
		changed = setConfigValue(target, 'botToken', params.botToken) || changed;
	}

	if (changed) {
		mkdirSync(dirname(configPath), { recursive: true });
		writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
		console.log('[OpenClawAgentV2] syncChannelConfig: openclaw.json written', {
			channelType: params.channelType,
			accountId: accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
			targetPath,
			configPath,
			allowFromCount: params.allowFrom?.length,
		});
	} else {
		console.log('[OpenClawAgentV2] syncChannelConfig: no changes needed', {
			channelType: params.channelType,
			accountId: accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
			targetPath,
			configPath,
			allowFromCount: params.allowFrom?.length,
		});
	}
	return { accountId, changed, configPath, targetPath };
}

function getNineRouterModelDefinition(model: string): IDataObject {
	return {
		id: model,
		name: model === 'auto' ? '9Router Auto' : model,
		api: NINE_ROUTER_API,
		reasoning: false,
		input: ['text', 'image'],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: NINE_ROUTER_CONTEXT_WINDOW,
		maxTokens: NINE_ROUTER_MAX_TOKENS,
	};
}

function syncNineRouterModelConfig(params: {
	modelConfig: ModelConfig | undefined;
	modelId: string | undefined;
}): {
	changed: boolean;
	configPath?: string;
	reason: string;
	targetPath?: string;
	modelRef?: string;
	existingBaseUrl?: string;
} {
	if (
		params.modelConfig?.modelSource !== NINE_ROUTER_MODEL_SOURCE ||
		normalizeLowercaseStringOrEmpty(getModelProvider(params.modelId)) !== NINE_ROUTER_PROVIDER ||
		!params.modelId
	) {
		return { changed: false, reason: 'not-nine-router' };
	}

	const modelRef = parseModelRef(params.modelId);
	if (!modelRef || modelRef.provider !== NINE_ROUTER_PROVIDER) {
		return { changed: false, reason: 'invalid-model-id' };
	}

	const configPath = getOpenClawConfigPath();
	if (!configPath) {
		throw new ApplicationError(
			`Could not determine OpenClaw config path. Set ${OPENCLAW_CONFIG_PATH_ENV} or HOME for the n8n process.`,
		);
	}

	const config = readOpenClawConfig(configPath);
	const models = ensureDataObject(config, 'models');
	const providers = ensureDataObject(models, 'providers');
	const provider = ensureDataObject(providers, NINE_ROUTER_PROVIDER);
	const existingBaseUrl = normalizeOptionalString(provider.baseUrl);

	let changed = false;
	changed =
		setDefaultConfigValue(
			provider,
			'baseUrl',
			normalizeOptionalString(params.modelConfig.extra?.baseUrl) ?? NINE_ROUTER_DEFAULT_BASE_URL,
		) || changed;
	changed = setConfigValue(provider, 'api', NINE_ROUTER_API) || changed;

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
		changed = setConfigValue(existingModel, 'api', NINE_ROUTER_API) || changed;
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
			setDefaultConfigValue(existingModel, 'contextWindow', NINE_ROUTER_CONTEXT_WINDOW) || changed;
		changed = setDefaultConfigValue(existingModel, 'maxTokens', NINE_ROUTER_MAX_TOKENS) || changed;
	} else {
		providerModels.push(getNineRouterModelDefinition(modelRef.model));
		changed = true;
	}

	const agents = ensureDataObject(config, 'agents');
	const defaults = ensureDataObject(agents, 'defaults');
	const defaultModels = ensureDataObject(defaults, 'models');
	const fullModelRef = `${modelRef.provider}/${modelRef.model}`;
	const existingDefaultModel = defaultModels[fullModelRef];
	if (!isObject(existingDefaultModel)) {
		defaultModels[fullModelRef] = {};
		changed = true;
	}

	if (changed) {
		mkdirSync(dirname(configPath), { recursive: true });
		writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
	}

	return {
		changed,
		configPath,
		reason: changed ? 'updated-provider' : 'already-current',
		targetPath: `models.providers.${NINE_ROUTER_PROVIDER}`,
		modelRef: fullModelRef,
		existingBaseUrl,
	};
}

function syncMcpServerConfig(params: { mcpServerConfigs: McpServerConfig[] }): {
	changed: boolean;
	configPath?: string;
	synced: string[];
	skipped: string[];
} {
	const synced: string[] = [];
	const skipped: string[] = [];
	if (params.mcpServerConfigs.length === 0) {
		return { changed: false, synced, skipped };
	}

	const configPath = getOpenClawConfigPath();
	if (!configPath) {
		const reason = `Could not determine OpenClaw config path. Set ${OPENCLAW_CONFIG_PATH_ENV} or HOME for the n8n process.`;
		console.log('[OpenClawAgentV2] syncMcpServerConfig: skipped all servers', {
			reason,
			serverCount: params.mcpServerConfigs.length,
		});
		return { changed: false, synced, skipped: [reason] };
	}

	console.log('[OpenClawAgentV2] syncMcpServerConfig: start', {
		configPath,
		serverCount: params.mcpServerConfigs.length,
		serverNames: params.mcpServerConfigs.map((server) => server.serverName),
	});

	const config = readOpenClawConfig(configPath);
	const mcp = ensureDataObject(config, 'mcp');
	const servers = ensureDataObject(mcp, 'servers');

	let changed = false;
	for (const serverConfig of params.mcpServerConfigs) {
		const serverName = normalizeOpenClawMcpServerName(serverConfig.serverName);
		const url = normalizeOptionalString(serverConfig.url);
		if (!serverName) {
			const reason = `invalid-name:${serverConfig.serverName}`;
			console.log('[OpenClawAgentV2] syncMcpServerConfig: skipping server', {
				rawServerName: serverConfig.serverName,
				reason,
			});
			skipped.push(reason);
			continue;
		}
		if (!url || !isHttpUrl(url)) {
			const reason = `invalid-url:${serverName}`;
			console.log('[OpenClawAgentV2] syncMcpServerConfig: skipping server', {
				serverName,
				hasUrl: !!url,
				reason,
			});
			skipped.push(reason);
			continue;
		}

		const server = ensureDataObject(servers, serverName);
		const transport = normalizeMcpTransport(serverConfig.transport);
		const headers =
			serverConfig.headers && Object.keys(serverConfig.headers).length > 0
				? serverConfig.headers
				: undefined;
		const connectionTimeoutMs = normalizePositiveInteger(serverConfig.connectionTimeoutMs);

		changed = setConfigValue(server, 'url', url) || changed;
		changed = setOptionalConfigValue(server, 'transport', transport) || changed;
		changed = setOptionalConfigValue(server, 'headers', headers) || changed;
		changed = setOptionalConfigValue(server, 'connectionTimeoutMs', connectionTimeoutMs) || changed;

		changed = deleteConfigValue(server, 'command') || changed;
		changed = deleteConfigValue(server, 'args') || changed;
		changed = deleteConfigValue(server, 'env') || changed;
		changed = deleteConfigValue(server, 'cwd') || changed;
		changed = deleteConfigValue(server, 'workingDirectory') || changed;

		console.log('[OpenClawAgentV2] syncMcpServerConfig: prepared server', {
			serverName,
			rawServerName: serverConfig.serverName,
			url,
			transport: transport ?? '(openclaw-default)',
			headerKeys: headers ? Object.keys(headers) : [],
			connectionTimeoutMs,
		});
		synced.push(serverName);
	}

	if (changed) {
		mkdirSync(dirname(configPath), { recursive: true });
		writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
		console.log('[OpenClawAgentV2] syncMcpServerConfig: openclaw.json written', {
			configPath,
			synced,
			skipped,
		});
	} else {
		console.log('[OpenClawAgentV2] syncMcpServerConfig: no changes needed', {
			configPath,
			synced,
			skipped,
		});
	}

	return { changed, configPath, synced, skipped };
}

function syncPluginConfig(params: {
	pluginConfigs: PluginConfig[];
}): { installed: string[]; skipped: string[]; errors: string[] } {
	const installed: string[] = [];
	const skipped: string[] = [];
	const errors: string[] = [];

	// Read current config to check what's already installed
	const configPath = getOpenClawConfigPath();
	let existingInstalls: Record<string, unknown> = {};
	let existingLoadPaths: string[] = [];
	let existingEntries: Record<string, unknown> = {};
	if (configPath) {
		try {
			const config = readOpenClawConfig(configPath);
			const plugins = config.plugins as IDataObject | undefined;
			existingInstalls = (plugins?.installs as Record<string, unknown>) ?? {};
			existingLoadPaths = ((plugins?.load as IDataObject)?.paths as string[]) ?? [];
			existingEntries = (plugins?.entries as Record<string, unknown>) ?? {};
		} catch {
			// Config doesn't exist yet — all plugins need install
		}
	}

	// Resolve the openclaw binary path (same logic as execute())
	const resolvedBinary = resolveOpenClawBinary('openclaw');
	const env = createOpenClawProcessEnv(resolvedBinary.pathDirectories, { NODE_NO_WARNINGS: '1' });

	for (const pluginCfg of params.pluginConfigs) {
		if (pluginCfg.pluginSource === 'local' && pluginCfg.pluginPath) {
			const pluginPath = pluginCfg.pluginPath;
			const manifestId = pluginCfg.pluginManifest?.id;

			// Check if already installed at this path
			if (manifestId) {
				const isPathLoaded = existingLoadPaths.includes(pluginPath);
				const isEntryEnabled = (existingEntries[manifestId] as IDataObject)?.enabled === true;

				if (isPathLoaded && isEntryEnabled) {
					console.log('[OpenClawAgentV2] syncPluginConfig: already installed, skipping', {
						manifestId,
						pluginPath,
					});
					skipped.push(manifestId);
					continue;
				}
			}

			// Not installed yet — run: openclaw plugins install -l <path>
			try {
				const cmd = `${resolvedBinary.binaryPath} plugins install -l ${JSON.stringify(pluginPath)}`;
				console.log('[OpenClawAgentV2] syncPluginConfig: running CLI', { cmd });
				const output = execSync(cmd, {
					timeout: 30_000,
					encoding: 'utf8',
					stdio: ['pipe', 'pipe', 'pipe'],
					env,
				});
				console.log('[OpenClawAgentV2] syncPluginConfig: CLI success', {
					pluginPath,
					manifestId,
					output: output.trim().slice(0, 500),
				});
				installed.push(manifestId ?? pluginPath);
			} catch (cliErr) {
				const errMsg = cliErr instanceof Error ? cliErr.message : String(cliErr);
				const stderr =
					cliErr && typeof cliErr === 'object' && 'stderr' in cliErr
						? String((cliErr as { stderr: unknown }).stderr)
								.trim()
								.slice(0, 500)
						: undefined;
				console.log('[OpenClawAgentV2] syncPluginConfig: CLI failed', {
					pluginPath,
					error: errMsg.slice(0, 300),
					stderr,
				});
				errors.push(`local:${pluginPath}: ${errMsg.slice(0, 200)}`);
			}
		} else if (pluginCfg.pluginSource === 'cloud' && pluginCfg.pluginId) {
			const pluginId = pluginCfg.pluginId;

			// Check if already installed
			const existingRecord = existingInstalls[pluginId] as { source?: string } | undefined;
			if (existingRecord?.source === 'npm' || existingRecord?.source === 'clawhub') {
				console.log('[OpenClawAgentV2] syncPluginConfig: already installed, skipping', {
					pluginId,
					source: existingRecord.source,
				});
				skipped.push(pluginId);
				continue;
			}

			const spec = pluginCfg.pluginVersion ? `${pluginId}@${pluginCfg.pluginVersion}` : pluginId;
			try {
				const cmd = `${resolvedBinary.binaryPath} plugins install ${JSON.stringify(spec)}`;
				console.log('[OpenClawAgentV2] syncPluginConfig: running CLI', { cmd });
				const output = execSync(cmd, {
					timeout: 60_000,
					encoding: 'utf8',
					stdio: ['pipe', 'pipe', 'pipe'],
					env,
				});
				console.log('[OpenClawAgentV2] syncPluginConfig: CLI success', {
					pluginId,
					output: output.trim().slice(0, 500),
				});
				installed.push(pluginId);
			} catch (cliErr) {
				const errMsg = cliErr instanceof Error ? cliErr.message : String(cliErr);
				console.log('[OpenClawAgentV2] syncPluginConfig: CLI failed', {
					pluginId,
					error: errMsg.slice(0, 300),
				});
				errors.push(`cloud:${pluginId}: ${errMsg.slice(0, 200)}`);
			}
		}
	}

	console.log('[OpenClawAgentV2] syncPluginConfig: result', {
		pluginCount: params.pluginConfigs.length,
		installedCount: installed.length,
		skippedCount: skipped.length,
		errorCount: errors.length,
		installed: installed.length > 0 ? installed : undefined,
		skipped: skipped.length > 0 ? skipped : undefined,
		errors: errors.length > 0 ? errors : undefined,
	});

	return { installed, skipped, errors };
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
		return { binaryPath, pathDirectories: [dirname(binaryPath)] };
	}
	for (const candidate of getDefaultBinarySearchPaths(binaryPath)) {
		if (isUsableFile(candidate)) {
			return { binaryPath: candidate, pathDirectories: [dirname(candidate)] };
		}
	}
	return { binaryPath, pathDirectories: [] };
}

function createOpenClawProcessEnv(
	pathDirectories: string[],
	additionalEnv: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
	const existingPath = process.env.PATH ?? '';
	const prependedPath = pathDirectories.filter(Boolean).join(delimiter);
	const nextPath = prependedPath ? `${prependedPath}${delimiter}${existingPath}` : existingPath;
	return { ...process.env, ...additionalEnv, PATH: nextPath };
}

function killOpenClawProcess(child: ChildProcess, signal: NodeJS.Signals): void {
	try {
		if (child.pid && process.platform !== 'win32') {
			process.kill(-child.pid, signal);
			return;
		}
	} catch {
		/* fallback */
	}
	try {
		child.kill(signal);
	} catch {
		/* already exited */
	}
}

function summarizeProcessOutput(output: string): string {
	const trimmed = output.trim();
	if (!trimmed) return '';
	const maxLength = 2000;
	return trimmed.length <= maxLength ? trimmed : `${trimmed.slice(0, maxLength)}...`;
}

function getWatchdogTimeoutMs(timeoutSeconds: number): number {
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
	if (/^[A-Za-z0-9_/:=-]+$/.test(value)) return value;
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
	env?: NodeJS.ProcessEnv;
	abortSignal?: AbortSignal;
}): Promise<OpenClawProcessResult> {
	return await new Promise<OpenClawProcessResult>((resolve, reject) => {
		const resolvedBinary = resolveOpenClawBinary(params.binaryPath);
		const command = getOpenClawCommand(resolvedBinary.binaryPath, params.args);
		const child = spawn(resolvedBinary.binaryPath, params.args, {
			cwd: params.cwd,
			detached: process.platform !== 'win32',
			stdio: ['ignore', 'pipe', 'pipe'],
			env: createOpenClawProcessEnv(resolvedBinary.pathDirectories, params.env),
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
			if (forceKillTimer) clearTimeout(forceKillTimer);
			params.abortSignal?.removeEventListener('abort', abortHandler);
			reject(
				new Error(
					`Failed to spawn OpenClaw CLI at "${resolvedBinary.binaryPath}": ${error.message}. Set Options > Binary Path to the full path of the openclaw executable, or set OPENCLAW_BINARY_PATH in the n8n process environment.`,
				),
			);
		});

		child.on('close', (exitCode: number | null, signal: NodeJS.Signals | null) => {
			clearTimeout(timeoutTimer);
			if (forceKillTimer) clearTimeout(forceKillTimer);
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

export class OpenClawAgentV2 implements INodeType {
	description: INodeTypeDescription;

	constructor(baseDescription: INodeTypeBaseDescription) {
		this.description = {
			...baseDescription,
			version: 2,
			subtitle:
				'={{$parameter.selectorType === "agent" ? "Agent: " + $parameter.agentId : $parameter.selectorType === "sessionId" ? "Session: " + $parameter.sessionId : $parameter.selectorType === "recipient" ? "To: " + $parameter.to : "Default route"}}',
			defaults: {
				name: 'OpenClaw AI Agent',
			},
			codex: {
				alias: ['OpenClaw', 'Agent', 'Gateway', 'Assistant', 'Channel'],
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
			inputs: [
				NodeConnectionTypes.Main,
				{
					type: NodeConnectionTypes.AiChannel,
					displayName: 'Channel',
				},
				{
					type: NodeConnectionTypes.AiLanguageModel,
					displayName: 'Model',
					required: false,
					maxConnections: 1,
				},
				{
					type: NodeConnectionTypes.AiTool,
					displayName: 'Plugin',
					required: false,
				},
				{
					type: NodeConnectionTypes.AiTool,
					displayName: 'MCP',
					required: false,
				},
			],
			outputs: [NodeConnectionTypes.Main],
			// No hardcoded credentials — channels provide their own
			properties: [
				{
					displayName:
						'Requires the OpenClaw CLI to be installed and configured on the n8n host. Connect Channel sub-nodes to configure messaging channels (Telegram, WhatsApp, etc.).',
					name: 'openClawNotice',
					type: 'notice',
					default: '',
				},
				{
					displayName: 'Message',
					name: 'message',
					type: 'string',
					required: true,
					default:
						'={{ $json.chatInput || $json.chat_input || $json.message || $json.text || "" }}',
					description: 'Message body to send to the OpenClaw agent',
					typeOptions: { rows: 5 },
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
					displayOptions: { show: { selectorType: ['agent'] } },
				},
				{
					displayName: 'Session ID',
					name: 'sessionId',
					type: 'string',
					default: '',
					description: 'OpenClaw session ID to continue',
					displayOptions: { show: { selectorType: ['sessionId'] } },
				},
				{
					displayName: 'Recipient',
					name: 'to',
					type: 'string',
					default: '',
					description: 'Recipient or channel target passed to OpenClaw as --to',
					displayOptions: { show: { selectorType: ['recipient'] } },
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
							description: 'Additional system instructions for this OpenClaw run',
							typeOptions: { rows: 5 },
						},
						{
							displayName: 'Binary Path',
							name: 'binaryPath',
							type: 'string',
							default: 'openclaw',
							description: 'Path to the openclaw binary',
						},
						{
							displayName: 'Working Directory',
							name: 'workingDirectory',
							type: 'string',
							default: '',
							description: 'Working directory for the OpenClaw process',
						},
						{
							displayName: 'Timeout',
							name: 'timeout',
							type: 'number',
							default: DEFAULT_TIMEOUT_SECONDS,
							description: 'OpenClaw agent timeout in seconds',
							typeOptions: { minValue: 1 },
						},
						{
							displayName: 'Verbose',
							name: 'verbose',
							type: 'options',
							default: '',
							description: 'Optional OpenClaw verbose setting',
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
	}

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const node = this.getNode();
		const workflowId = this.getWorkflow().id;
		const activationMode = this.getActivationMode();
		console.log('[OpenClawAgentV2] trigger activation registered', {
			nodeName: node.name,
			workflowId,
			activationMode,
		});
		console.log('OpenClaw publish sync: trigger activated', {
			workflowId,
			activationMode,
			nodeName: node.name,
		});

		// ── Publish-time config sync ──
		// On activation (publish), discover connected sub-nodes and sync
		// their configs to openclaw.json so the CLI has up-to-date settings.
		if (activationMode !== 'init') {
			try {
				const nodeName = node.name;

				// Find OpenClaw config sub-nodes connected via AiTool.
				const aiToolParents = this.getParentNodes(nodeName, {
					includeNodeParameters: true,
					connectionType: NodeConnectionTypes.AiTool,
					depth: 1,
				});
				console.log('OpenClaw publish sync: discovered AI tool config sub-nodes', {
					count: aiToolParents.length,
					nodes: aiToolParents.map((p) => ({
						name: (p as unknown as { name?: string }).name,
						type: p.type,
						params: p.parameters ? Object.keys(p.parameters) : [],
					})),
				});

				// Build PluginConfig and McpServerConfig objects from discovered sub-node parameters.
				const publishPluginConfigs: PluginConfig[] = [];
				const publishMcpServerConfigs: McpServerConfig[] = [];
				for (const pluginNode of aiToolParents) {
					const params = pluginNode.parameters ?? {};
					const mcpServerConfig = getMcpServerConfigFromParameters(
						params as Record<string, unknown>,
					);
					if (mcpServerConfig.config) {
						console.log('OpenClaw publish sync: processing MCP Server sub-node', {
							type: pluginNode.type,
							serverName: mcpServerConfig.config.serverName,
							url: mcpServerConfig.config.url,
							transport: mcpServerConfig.config.transport ?? '(openclaw-default)',
							headerKeys: mcpServerConfig.config.headers
								? Object.keys(mcpServerConfig.config.headers)
								: [],
						});
						publishMcpServerConfigs.push(mcpServerConfig.config);
						continue;
					} else if (mcpServerConfig.reason !== 'not-mcp-server-node') {
						console.log('OpenClaw publish sync: skipping MCP Server sub-node', {
							type: pluginNode.type,
							reason: mcpServerConfig.reason,
							params: JSON.stringify(params).slice(0, 300),
						});
						continue;
					}

					const pluginSource = (params.pluginSource as string) || 'local';

					console.log('OpenClaw publish sync: processing plugin sub-node', {
						type: pluginNode.type,
						pluginSource,
						params: JSON.stringify(params).slice(0, 300),
					});

					if (pluginSource === 'local') {
						// Read pluginDirectory — may be an expression like ={{ $workspace.__dirPath }}
						let pluginDirectory = (params.pluginDirectory as string) || '';

						// If it's an expression referencing $workspace.__dirPath, resolve it manually
						// from the workflow's workspace context (available at trigger/activation time)
						if (
							pluginDirectory.includes('$workspace.__dirPath') ||
							pluginDirectory.includes('$workspace[')
						) {
							// Access the workflow's workspace context directly
							// The underlying Workflow object is available through the TriggerContext
							// cast to access the protected 'workflow' field
							const workflowObj = (this as unknown as { workflow: { workspace?: IDataObject } })
								.workflow;
							const dirPath = normalizeOptionalString(workflowObj?.workspace?.__dirPath);
							console.log('OpenClaw publish sync: resolving $workspace.__dirPath expression', {
								rawValue: pluginDirectory,
								workspaceAvailable: !!workflowObj?.workspace,
								dirPath: dirPath ?? '(not set)',
							});
							if (dirPath) {
								pluginDirectory = dirPath;
							} else {
								console.log(
									'OpenClaw publish sync: $workspace.__dirPath is not available, skipping',
								);
								continue;
							}
						} else if (pluginDirectory.includes('{{') || pluginDirectory.startsWith('=')) {
							// Other expressions we can't resolve at activation time
							console.log(
								'OpenClaw publish sync: pluginDirectory is an unresolvable expression, skipping',
								{
									rawValue: pluginDirectory,
								},
							);
							continue;
						}
						pluginDirectory = pluginDirectory.trim();
						if (!pluginDirectory) {
							console.log('OpenClaw publish sync: pluginDirectory is empty, skipping');
							continue;
						}

						// Scan for manifest
						const manifestPath = join(pluginDirectory, 'openclaw.plugin.json');
						let pluginManifest: PluginConfig['pluginManifest'];
						if (existsSync(manifestPath)) {
							try {
								const raw = readFileSync(manifestPath, 'utf8').trim();
								const parsed = JSON.parse(raw) as IDataObject;
								pluginManifest = {
									id: typeof parsed.id === 'string' ? parsed.id : undefined,
									name: typeof parsed.name === 'string' ? parsed.name : undefined,
									description:
										typeof parsed.description === 'string' ? parsed.description : undefined,
									version: typeof parsed.version === 'string' ? parsed.version : undefined,
									providers: Array.isArray(parsed.providers)
										? (parsed.providers as unknown[]).filter(
												(p): p is string => typeof p === 'string',
											)
										: undefined,
									channels: Array.isArray(parsed.channels)
										? (parsed.channels as unknown[]).filter(
												(c): c is string => typeof c === 'string',
											)
										: undefined,
								};
								console.log('OpenClaw publish sync: loaded local manifest', {
									manifestPath,
									id: pluginManifest.id,
									name: pluginManifest.name,
								});
							} catch (parseErr) {
								console.log('OpenClaw publish sync: failed to parse manifest', {
									manifestPath,
									error: getErrorMessage(parseErr),
								});
							}
						}

						publishPluginConfigs.push({
							pluginSource: 'local',
							pluginPath: pluginDirectory,
							pluginManifest,
						});
					} else if (pluginSource === 'cloud') {
						const pluginId = ((params.pluginId as string) || '').trim();
						const pluginVersion = ((params.pluginVersion as string) || '').trim() || undefined;
						if (pluginId) {
							publishPluginConfigs.push({
								pluginSource: 'cloud',
								pluginId,
								pluginVersion,
							});
						}
					}
				}

				// Sync plugin configs to openclaw.json
				if (publishPluginConfigs.length > 0) {
					try {
						const syncResult = syncPluginConfig({ pluginConfigs: publishPluginConfigs });
						console.log('OpenClaw publish sync: plugin config synced on activation', {
							installedCount: syncResult.installed.length,
							installed: syncResult.installed,
							errors: syncResult.errors.length > 0 ? syncResult.errors : undefined,
							pluginCount: publishPluginConfigs.length,
						});
					} catch (syncErr) {
						console.log('OpenClaw publish sync: plugin config sync failed on activation', {
							error: getErrorMessage(syncErr),
						});
					}
				} else {
					console.log('OpenClaw publish sync: no plugin configs to sync on activation');
				}

				if (publishMcpServerConfigs.length > 0) {
					try {
						const syncResult = syncMcpServerConfig({
							mcpServerConfigs: publishMcpServerConfigs,
						});
						console.log('OpenClaw publish sync: MCP Server config synced on activation', {
							changed: syncResult.changed,
							configPath: syncResult.configPath,
							synced: syncResult.synced,
							skipped: syncResult.skipped.length > 0 ? syncResult.skipped : undefined,
							serverCount: publishMcpServerConfigs.length,
						});
					} catch (syncErr) {
						console.log('OpenClaw publish sync: MCP Server config sync failed on activation', {
							error: getErrorMessage(syncErr),
						});
					}
				} else {
					console.log('OpenClaw publish sync: no MCP Server configs to sync on activation');
				}
			} catch (publishErr) {
				console.log('OpenClaw publish sync: activation-time config sync failed (non-fatal)', {
					error: getErrorMessage(publishErr),
				});
			}
		}

		return {
			closeFunction: async () => {
				console.log('[OpenClawAgentV2] trigger activation closed', {
					nodeName: node.name,
					workflowId,
				});
				console.log('OpenClaw publish sync: trigger deactivated', {
					workflowId,
					nodeName: node.name,
				});
			},
		};
	}

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		console.log('OpenClaw publish sync: execute started', {
			itemCount: items.length,
			nodeName: this.getNode().name,
			workflowId: this.getWorkflow().id,
		});

		// Retrieve channel configs from connected sub-nodes
		let channelConfigs: ChannelConfig[] = [];
		try {
			const channelData = await this.getInputConnectionData(NodeConnectionTypes.AiChannel, 0);
			if (Array.isArray(channelData)) {
				channelConfigs = channelData as ChannelConfig[];
			} else if (channelData && isObject(channelData)) {
				channelConfigs = [channelData as unknown as ChannelConfig];
			}
			console.log('OpenClaw publish sync: resolved channel candidates', {
				channelCount: channelConfigs.length,
				channelTypes: channelConfigs.map((c) => c.channelType),
				channels: channelConfigs.map((channelConfig) => ({
					channelType: channelConfig.channelType,
					accountId: channelConfig.accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
					dmPolicy: channelConfig.dmPolicy,
					groupPolicy: channelConfig.groupPolicy,
					allowFromCount: channelConfig.allowFrom?.length,
				})),
			});
		} catch {
			// No channels connected — that's fine
			console.log('OpenClaw publish sync: no channels connected');
		}

		// Retrieve model config from connected Model sub-node
		let modelConfig: ModelConfig | undefined;
		try {
			const modelData = await this.getInputConnectionData(NodeConnectionTypes.AiLanguageModel, 0);
			console.log('[OpenClawAgentV2] Raw model data from getInputConnectionData', {
				type: typeof modelData,
				isArray: Array.isArray(modelData),
				isObject: isObject(modelData),
				constructorName:
					modelData && typeof modelData === 'object'
						? (modelData as object).constructor?.name
						: undefined,
				keys:
					modelData && typeof modelData === 'object' && !Array.isArray(modelData)
						? Object.keys(modelData as object)
						: undefined,
				preview: getModelDataPreview(modelData),
			});

			// Extract the model config — may come as a single object or wrapped in an array
			let candidate: unknown = modelData;
			if (Array.isArray(modelData) && modelData.length > 0) {
				candidate = modelData[0];
				console.log('[OpenClawAgentV2] Model data was array, using first element', {
					arrayLength: modelData.length,
					elementType: typeof candidate,
				});
			}

			if (
				candidate &&
				isObject(candidate) &&
				typeof (candidate as Record<string, unknown>).modelId === 'string'
			) {
				modelConfig = candidate as unknown as ModelConfig;
				console.log('[OpenClawAgentV2] Model sub-node connected', {
					modelId: modelConfig.modelId,
					modelSource: modelConfig.modelSource,
					extraKeys: getObjectKeys(modelConfig.extra),
					baseUrl: normalizeOptionalString(modelConfig.extra?.baseUrl),
					api: normalizeOptionalString(modelConfig.extra?.api),
				});
			} else if (candidate) {
				// Data came back but doesn't match ModelConfig shape — log for debugging
				console.log('[OpenClawAgentV2] Model data received but does not match ModelConfig shape', {
					candidateType: typeof candidate,
					isObj: isObject(candidate),
					hasModelId: isObject(candidate)
						? typeof (candidate as Record<string, unknown>).modelId
						: 'n/a',
					candidateKeys: isObject(candidate) ? Object.keys(candidate as object) : [],
					candidatePreview: getModelDataPreview(candidate),
				});
			}
		} catch (err) {
			// No model connected — that's fine, will use the parameter
			console.log('[OpenClawAgentV2] No Model sub-node connected, using text parameter fallback', {
				error: err instanceof Error ? err.message : String(err),
			});
		}
		console.log('OpenClaw publish sync: resolved model sync candidates', {
			hasModelSubNode: !!modelConfig,
			modelId: modelConfig?.modelId,
			modelSource: modelConfig?.modelSource ?? 'none',
		});

		// Retrieve plugin configs from connected Plugin sub-nodes
		let pluginConfigs: PluginConfig[] = [];
		let mcpServerConfigs: McpServerConfig[] = [];
		console.log('[OpenClawAgentV2] About to retrieve OpenClaw AiTool sub-node data');
		try {
			const pluginData = await this.getInputConnectionData(NodeConnectionTypes.AiTool, 0);
			console.log('[OpenClawAgentV2] Raw AiTool data from getInputConnectionData', {
				type: typeof pluginData,
				isNull: pluginData === null,
				isUndefined: pluginData === undefined,
				isArray: Array.isArray(pluginData),
				isObject: isObject(pluginData),
				preview: pluginData ? JSON.stringify(pluginData).slice(0, 500) : '(empty)',
			});

			const isPluginConfig = (item: unknown): item is PluginConfig =>
				isObject(item) && typeof (item as Record<string, unknown>).pluginSource === 'string';
			const isMcpServerConfig = (item: unknown): item is McpServerConfig =>
				isObject(item) &&
				(item as Record<string, unknown>).mcpServerSource === 'openclaw' &&
				typeof (item as Record<string, unknown>).serverName === 'string' &&
				typeof (item as Record<string, unknown>).url === 'string';

			if (Array.isArray(pluginData)) {
				pluginConfigs = pluginData.filter(isPluginConfig);
				mcpServerConfigs = pluginData.filter(isMcpServerConfig);
			} else if (pluginData && isPluginConfig(pluginData)) {
				pluginConfigs = [pluginData];
			} else if (pluginData && isMcpServerConfig(pluginData)) {
				mcpServerConfigs = [pluginData];
			}

			if (pluginConfigs.length > 0) {
				console.log('[OpenClawAgentV2] Plugin sub-nodes connected', {
					count: pluginConfigs.length,
					plugins: pluginConfigs.map((p) => ({
						source: p.pluginSource,
						path: p.pluginPath,
						manifestId: p.pluginManifest?.id,
						manifestName: p.pluginManifest?.name,
						cloudId: p.pluginId,
						cloudVersion: p.pluginVersion,
					})),
				});
			} else {
				console.log('[OpenClawAgentV2] No valid Plugin sub-nodes connected');
			}

			if (mcpServerConfigs.length > 0) {
				console.log('[OpenClawAgentV2] MCP Server sub-nodes connected', {
					count: mcpServerConfigs.length,
					servers: mcpServerConfigs.map((server) => ({
						serverName: server.serverName,
						url: server.url,
						transport: server.transport ?? '(openclaw-default)',
						headerKeys: server.headers ? Object.keys(server.headers) : [],
						connectionTimeoutMs: server.connectionTimeoutMs,
					})),
				});
			} else {
				console.log('[OpenClawAgentV2] No valid MCP Server sub-nodes connected');
			}
		} catch (pluginErr) {
			// Log the actual error — this is critical for troubleshooting
			console.log('[OpenClawAgentV2] OpenClaw AiTool sub-node connection FAILED', {
				errorType: pluginErr instanceof Error ? pluginErr.constructor.name : typeof pluginErr,
				errorMessage: pluginErr instanceof Error ? pluginErr.message : String(pluginErr),
				errorStack:
					pluginErr instanceof Error
						? pluginErr.stack?.split('\n').slice(0, 5).join('\n')
						: undefined,
			});
			console.log(
				'[OpenClawAgentV2] No Plugin or MCP Server sub-nodes connected (AiTool input error)',
			);
		}
		console.log('OpenClaw publish sync: resolved plugin sync candidates', {
			pluginCount: pluginConfigs.length,
			localCount: pluginConfigs.filter((p) => p.pluginSource === 'local').length,
			cloudCount: pluginConfigs.filter((p) => p.pluginSource === 'cloud').length,
			manifestIds: pluginConfigs
				.filter((p) => p.pluginManifest?.id)
				.map((p) => p.pluginManifest?.id),
			cloudIds: pluginConfigs.filter((p) => p.pluginId).map((p) => p.pluginId),
		});
		console.log('OpenClaw publish sync: resolved MCP Server sync candidates', {
			serverCount: mcpServerConfigs.length,
			serverNames: mcpServerConfigs.map((server) => server.serverName),
			transports: mcpServerConfigs.map((server) => server.transport ?? '(openclaw-default)'),
		});

		if (mcpServerConfigs.length > 0) {
			try {
				const syncResult = syncMcpServerConfig({ mcpServerConfigs });
				console.log('OpenClaw publish sync: MCP Server config sync completed before execute', {
					changed: syncResult.changed,
					configPath: syncResult.configPath,
					synced: syncResult.synced,
					skipped: syncResult.skipped.length > 0 ? syncResult.skipped : undefined,
				});
			} catch (mcpSyncErr) {
				console.log('OpenClaw publish sync: MCP Server config sync failed before execute', {
					error: getErrorMessage(mcpSyncErr),
				});
			}
		}

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				const message = normalizeOptionalString(this.getNodeParameter('message', itemIndex));
				if (!message) {
					throw new NodeOperationError(this.getNode(), 'Message must not be empty', { itemIndex });
				}

				const openClawEnv: NodeJS.ProcessEnv = {};

				// Plugin env vars for CLI (config sync happens at publish time in trigger())
				if (pluginConfigs.length > 0) {
					const localPlugins = pluginConfigs.filter((p) => p.pluginSource === 'local');
					const cloudPlugins = pluginConfigs.filter((p) => p.pluginSource === 'cloud');

					const pluginPaths = localPlugins
						.map((p) => p.pluginPath)
						.filter((p): p is string => typeof p === 'string' && p.length > 0);

					const cloudSpecs = cloudPlugins
						.map((p) => {
							if (!p.pluginId) return undefined;
							return p.pluginVersion
								? `clawhub:${p.pluginId}@${p.pluginVersion}`
								: `clawhub:${p.pluginId}`;
						})
						.filter((s): s is string => typeof s === 'string');

					// Pass local plugin paths via environment variable for the CLI
					if (pluginPaths.length > 0) {
						openClawEnv.OPENCLAW_PLUGIN_PATHS = pluginPaths.join(';');
						console.log('[OpenClawAgentV2] Set OPENCLAW_PLUGIN_PATHS env var', {
							itemIndex,
							value: openClawEnv.OPENCLAW_PLUGIN_PATHS,
						});
					}

					// Pass cloud plugin specs via environment variable for the CLI
					if (cloudSpecs.length > 0) {
						openClawEnv.OPENCLAW_CLAWHUB_PLUGINS = cloudSpecs.join(';');
						console.log('[OpenClawAgentV2] Set OPENCLAW_CLAWHUB_PLUGINS env var', {
							itemIndex,
							value: openClawEnv.OPENCLAW_CLAWHUB_PLUGINS,
						});
					}
				}

				// Apply channel configs
				const telegramChannel = channelConfigs.find((c) => c.channelType === 'telegram');
				const primaryChannel = channelConfigs[0];

				if (telegramChannel?.botToken) {
					openClawEnv.TELEGRAM_BOT_TOKEN = telegramChannel.botToken;
					const telegramSync = syncChannelConfig({
						channelType: 'telegram',
						botToken: telegramChannel.botToken,
						replyAccount: telegramChannel.accountId,
						dmPolicy: telegramChannel.dmPolicy,
						groupPolicy: telegramChannel.groupPolicy,
						allowFrom: telegramChannel.allowFrom ?? [],
					});
					console.log('OpenClaw publish sync: Telegram channel config synced', {
						itemIndex,
						changed: telegramSync.changed,
						configPath: telegramSync.configPath,
						targetPath: telegramSync.targetPath,
						accountId: telegramSync.accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
						allowFromCount: telegramChannel.allowFrom?.length ?? 0,
					});
				}

				// Apply WhatsApp channel config if present
				const whatsappChannel = channelConfigs.find((c) => c.channelType === 'whatsapp');
				if (whatsappChannel?.accessToken) {
					openClawEnv.WHATSAPP_ACCESS_TOKEN = whatsappChannel.accessToken;
					if (whatsappChannel.phoneNumberId) {
						openClawEnv.WHATSAPP_PHONE_NUMBER_ID = whatsappChannel.phoneNumberId;
					}
					const whatsappSync = syncChannelConfig({
						channelType: 'whatsapp',
						botToken: whatsappChannel.accessToken,
						replyAccount: whatsappChannel.accountId,
					});
					console.log('OpenClaw publish sync: WhatsApp channel config synced', {
						itemIndex,
						changed: whatsappSync.changed,
						configPath: whatsappSync.configPath,
						targetPath: whatsappSync.targetPath,
						accountId: whatsappSync.accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
					});
				}
				console.log('OpenClaw publish sync: channel config sync completed', {
					itemIndex,
					hasTelegram: !!telegramChannel,
					hasWhatsApp: !!whatsappChannel,
					channelCount: channelConfigs.length,
				});

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

				// Resolve model from connected Model sub-node
				const resolvedModel = modelConfig?.modelId;
				console.log('[OpenClawAgentV2] Model resolution', {
					itemIndex,
					connectedModel: modelConfig?.modelId,
					resolvedModel,
					modelSource: modelConfig?.modelSource ?? 'none',
				});
				if (resolvedModel) {
					args.push('--model', resolvedModel);
					gatewayParams.model = resolvedModel;
				}

				const openCodeFreeAuth = applyOpenCodeFreeAuthEnv({
					env: openClawEnv,
					modelConfig,
					modelId: resolvedModel,
				});
				console.log('[OpenClawAgentV2] OpenCode free auth env resolution', {
					itemIndex,
					resolvedModel,
					modelSource: modelConfig?.modelSource ?? 'text-parameter',
					applied: openCodeFreeAuth.applied,
					envVar: openCodeFreeAuth.envVar,
					reason: openCodeFreeAuth.reason,
				});

				const nineRouterConfigSync = syncNineRouterModelConfig({
					modelConfig,
					modelId: resolvedModel,
				});
				console.log('[OpenClawAgentV2] 9Router model config sync', {
					itemIndex,
					hasModelSubNode: !!modelConfig,
					resolvedModel,
					modelSource: modelConfig?.modelSource ?? 'text-parameter',
					selectorType,
					changed: nineRouterConfigSync.changed,
					reason: nineRouterConfigSync.reason,
					targetPath: nineRouterConfigSync.targetPath,
					modelRef: nineRouterConfigSync.modelRef,
					configPath: nineRouterConfigSync.configPath,
					existingBaseUrl: nineRouterConfigSync.existingBaseUrl,
					restart: 'publish-only',
				});
				console.log('OpenClaw publish sync: model config sync completed', {
					itemIndex,
					resolvedModel,
					modelSource: modelConfig?.modelSource ?? 'text-parameter',
					syncChanged: nineRouterConfigSync.changed,
					syncReason: nineRouterConfigSync.reason,
				});

				const thinking = normalizeOptionalString(this.getNodeParameter('thinking', itemIndex));
				if (thinking) {
					args.push('--thinking', thinking);
					gatewayParams.thinking = thinking;
				}

				const runLocally = this.getNodeParameter('local', itemIndex, false) === true;
				if (runLocally) args.push('--local');

				const deliverReply = this.getNodeParameter('deliver', itemIndex, false) === true;
				if (deliverReply) {
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

				// Auto-set reply channel from connected channels
				if (deliverReply && primaryChannel) {
					const replyChannel = primaryChannel.channelType;
					args.push('--reply-channel', replyChannel);
					gatewayParams.replyChannel = replyChannel;

					if (primaryChannel.accountId) {
						args.push('--reply-account', primaryChannel.accountId);
						gatewayParams.replyAccountId = primaryChannel.accountId;
					}
				}

				const verbose = normalizeOptionalString(
					this.getNodeParameter('options.verbose', itemIndex, ''),
				);
				if (verbose) args.push('--verbose', verbose);

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
					if (runLocally) {
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

				console.log('OpenClaw publish sync: launching CLI', {
					itemIndex,
					binaryPath,
					argCount: args.length,
					cwd: workingDirectory ?? '(default)',
					envKeys: Object.keys(openClawEnv),
					resolvedModel,
					pluginCount: pluginConfigs.length,
					mcpServerCount: mcpServerConfigs.length,
					channelCount: channelConfigs.length,
				});

				const result = await runOpenClawCli({
					binaryPath,
					args,
					cwd: workingDirectory,
					timeoutMs: processTimeoutMs ?? getWatchdogTimeoutMs(timeoutSeconds),
					env: openClawEnv,
					abortSignal: this.getExecutionCancelSignal(),
				});

				console.log('OpenClaw publish sync: CLI execution completed', {
					itemIndex,
					exitCode: result.exitCode,
					signal: result.signal,
					stdoutLength: result.stdout.length,
					stderrLength: result.stderr.length,
				});

				if (result.exitCode !== 0) {
					throw new NodeOperationError(
						this.getNode(),
						result.stderr.trim() ||
							result.stdout.trim() ||
							`OpenClaw CLI exited with code ${result.exitCode ?? `signal ${result.signal}`}`,
						{ itemIndex },
					);
				}

				const json = parseOpenClawOutput(result.stdout);
				json.command = result.command;
				if (this.getNodeParameter('options.includeRawOutput', itemIndex, false) === true) {
					json.rawOutput = { stdout: result.stdout, stderr: result.stderr };
				}
				returnData.push({ json, pairedItem: { item: itemIndex } });
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
