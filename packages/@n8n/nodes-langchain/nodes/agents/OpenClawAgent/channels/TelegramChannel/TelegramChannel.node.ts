import { existsSync, readFileSync, statSync } from 'fs';
import { join } from 'path';

import {
	NodeConnectionTypes,
	type ILoadOptionsFunctions,
	type INodeType,
	type INodeTypeDescription,
	type INodePropertyOptions,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { ChannelConfig } from '../../V2/OpenClawAgentV2.node';

const TELEGRAM_CREDENTIAL_TYPE = 'telegramApi';
const OPENCLAW_STATE_DIR_ENV = 'OPENCLAW_STATE_DIR';
const OPENCLAW_OAUTH_DIR_ENV = 'OPENCLAW_OAUTH_DIR';
const OPENCLAW_DEFAULT_ACCOUNT_ID = 'default';

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeOptionalString(value: unknown): string | undefined {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return String(value);
	}
	if (typeof value !== 'string') {
		return undefined;
	}
	const trimmed = value.trim();
	return trimmed || undefined;
}

function normalizeTelegramAllowFromEntry(value: unknown): string | undefined {
	const raw = normalizeOptionalString(value);
	if (!raw) {
		return undefined;
	}
	const normalized = raw.replace(/^(telegram|tg):/i, '').trim();
	return normalized || undefined;
}

function normalizeAllowFromEntries(value: unknown): string[] {
	const rawEntries = Array.isArray(value)
		? value
		: typeof value === 'string'
			? value.split(/[\n,]/)
			: [];
	const seen = new Set<string>();
	const entries: string[] = [];
	for (const rawEntry of rawEntries) {
		const entry = normalizeTelegramAllowFromEntry(rawEntry);
		if (!entry || seen.has(entry)) {
			continue;
		}
		seen.add(entry);
		entries.push(entry);
	}
	return entries;
}

function getHomeDirectory(): string | undefined {
	return (
		normalizeOptionalString(process.env.HOME) ?? normalizeOptionalString(process.env.USERPROFILE)
	);
}

function resolveOpenClawStateDir(): string | undefined {
	const stateDir = normalizeOptionalString(process.env[OPENCLAW_STATE_DIR_ENV]);
	if (stateDir) {
		return stateDir;
	}
	const home = getHomeDirectory();
	return home ? join(home, '.openclaw') : undefined;
}

function resolveOpenClawCredentialsDir(): string | undefined {
	const oauthDir = normalizeOptionalString(process.env[OPENCLAW_OAUTH_DIR_ENV]);
	if (oauthDir) {
		return oauthDir;
	}
	const stateDir = resolveOpenClawStateDir();
	return stateDir ? join(stateDir, 'credentials') : undefined;
}

function normalizeOpenClawAccountId(value: unknown): string {
	const accountId = normalizeOptionalString(value)?.toLowerCase();
	return accountId || OPENCLAW_DEFAULT_ACCOUNT_ID;
}

function safeFilenameKey(value: string): string {
	return value
		.trim()
		.toLowerCase()
		.replace(/[\\/:*?"<>|]/g, '_')
		.replace(/\.\./g, '_');
}

function getTelegramPairingPath(credentialsDir: string): string {
	return join(credentialsDir, 'telegram-pairing.json');
}

function getTelegramAllowFromPaths(credentialsDir: string, accountId: string): string[] {
	const accountPath = join(credentialsDir, `telegram-${safeFilenameKey(accountId)}-allowFrom.json`);
	if (accountId === OPENCLAW_DEFAULT_ACCOUNT_ID) {
		return [accountPath, join(credentialsDir, 'telegram-allowFrom.json')];
	}
	return [accountPath];
}

function readJsonFile(filePath: string): unknown | undefined {
	try {
		if (!existsSync(filePath) || !statSync(filePath).isFile()) {
			return undefined;
		}
		return JSON.parse(readFileSync(filePath, 'utf8'));
	} catch (error) {
		console.log('[TelegramChannel] Failed to read Allow From source file', {
			filePath,
			error: error instanceof Error ? error.message : String(error),
		});
		return undefined;
	}
}

function getMetaString(meta: unknown, key: string): string | undefined {
	return isObject(meta) ? normalizeOptionalString(meta[key]) : undefined;
}

function formatPairingOptionName(request: Record<string, unknown>): string {
	const id = normalizeTelegramAllowFromEntry(request.id) ?? 'Unknown sender';
	const meta = request.meta;
	const username = getMetaString(meta, 'username');
	const firstName = getMetaString(meta, 'firstName');
	const lastName = getMetaString(meta, 'lastName');
	const fullName = [firstName, lastName].filter(Boolean).join(' ');
	const labelParts = [username ? `@${username}` : undefined, fullName || undefined].filter(Boolean);
	return labelParts.length > 0 ? `${id} (${labelParts.join(', ')})` : id;
}

function addOption(
	options: INodePropertyOptions[],
	seen: Set<string>,
	option: INodePropertyOptions,
): void {
	const value = String(option.value);
	if (seen.has(value)) {
		return;
	}
	seen.add(value);
	options.push(option);
}

function getTelegramAllowFromOptions(accountId: string): INodePropertyOptions[] {
	const credentialsDir = resolveOpenClawCredentialsDir();
	if (!credentialsDir) {
		console.log('[TelegramChannel] Allow From options skipped: OpenClaw credentials dir unknown', {
			accountId,
		});
		return [];
	}

	const pairingPath = getTelegramPairingPath(credentialsDir);
	const allowFromPaths = getTelegramAllowFromPaths(credentialsDir, accountId);
	console.log('[TelegramChannel] Loading Allow From options', {
		accountId,
		credentialsDir,
		pairingPath,
		allowFromPaths,
	});

	const options: INodePropertyOptions[] = [];
	const seen = new Set<string>();
	const pairingStore = readJsonFile(pairingPath);
	const requests =
		isObject(pairingStore) && Array.isArray(pairingStore.requests) ? pairingStore.requests : [];
	let pairingOptionCount = 0;

	for (const request of requests) {
		if (!isObject(request)) {
			continue;
		}
		const id = normalizeTelegramAllowFromEntry(request.id);
		if (!id) {
			continue;
		}
		const requestAccountId = normalizeOpenClawAccountId(getMetaString(request.meta, 'accountId'));
		if (requestAccountId !== accountId) {
			continue;
		}
		addOption(options, seen, {
			name: formatPairingOptionName(request),
			value: id,
			description: `Pending Telegram pairing request from ${pairingPath}`,
		});
		pairingOptionCount++;
	}

	let allowFromOptionCount = 0;
	for (const allowFromPath of allowFromPaths) {
		const allowFromStore = readJsonFile(allowFromPath);
		const entries =
			isObject(allowFromStore) && Array.isArray(allowFromStore.allowFrom)
				? normalizeAllowFromEntries(allowFromStore.allowFrom)
				: [];
		for (const entry of entries) {
			addOption(options, seen, {
				name: `${entry} (Configured)`,
				value: entry,
				description: `Existing Telegram allowFrom entry from ${allowFromPath}`,
			});
			allowFromOptionCount++;
		}
	}

	console.log('[TelegramChannel] Loaded Allow From options', {
		accountId,
		optionCount: options.length,
		pairingOptionCount,
		allowFromOptionCount,
	});
	return options;
}

export class TelegramChannel implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Telegram Channel',
		name: 'openClawTelegramChannel',
		icon: 'file:telegram-channel.svg',
		iconColor: 'blue',
		group: ['transform'],
		version: 1,
		description: 'Provides Telegram channel configuration to an OpenClaw AI Agent',
		defaults: {
			name: 'Telegram Channel',
		},
		codex: {
			alias: ['Telegram', 'Channel', 'Bot', 'OpenClaw'],
			categories: ['AI'],
			subcategories: {
				AI: ['Other'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://docs.openclaw.ai/channels/telegram',
					},
				],
			},
		},
		inputs: [],
		outputs: [NodeConnectionTypes.AiChannel],
		outputNames: ['Channel'],
		credentials: [
			{
				name: TELEGRAM_CREDENTIAL_TYPE,
				required: true,
			},
		],
		properties: [
			{
				displayName:
					'Connect this node to an OpenClaw AI Agent to provide Telegram channel configuration. The bot token is read from the Telegram credential.',
				name: 'telegramNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Account ID',
				name: 'accountId',
				type: 'string',
				default: '',
				description:
					'Optional OpenClaw account ID for multi-account setups. Leave empty for the default account.',
			},
			{
				displayName: 'DM Policy',
				name: 'dmPolicy',
				type: 'options',
				default: 'pairing',
				description: 'Direct message policy for this Telegram channel',
				options: [
					{ name: 'Pairing', value: 'pairing' },
					{ name: 'Open', value: 'open' },
					{ name: 'Allowlist', value: 'allowlist' },
					{ name: 'Disabled', value: 'disabled' },
				],
			},
			{
				displayName: 'Allow From Names or IDs',
				name: 'allowFrom',
				type: 'multiOptions',
				typeOptions: {
					loadOptionsMethod: 'getAllowFromOptions',
					loadOptionsDependsOn: ['accountId'],
				},
				default: [],
				allowArbitraryValues: true,
				description:
					'Choose from the list, or specify IDs using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
			},
			{
				displayName: 'Group Policy',
				name: 'groupPolicy',
				type: 'options',
				default: 'allowlist',
				description: 'Group message policy for this Telegram channel',
				options: [
					{ name: 'Allowlist', value: 'allowlist' },
					{ name: 'Open', value: 'open' },
					{ name: 'Disabled', value: 'disabled' },
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const credentials = await this.getCredentials<{ accessToken?: string }>(
			TELEGRAM_CREDENTIAL_TYPE,
			itemIndex,
		);

		const botToken = credentials.accessToken?.trim();
		const accountId =
			(this.getNodeParameter('accountId', itemIndex, '') as string).trim() || undefined;
		const dmPolicy = this.getNodeParameter('dmPolicy', itemIndex, 'pairing') as string;
		const allowFrom = normalizeAllowFromEntries(this.getNodeParameter('allowFrom', itemIndex, []));
		const groupPolicy = this.getNodeParameter('groupPolicy', itemIndex, 'allowlist') as string;

		console.log('[TelegramChannel] supplyData resolved channel config', {
			itemIndex,
			hasBotToken: !!botToken,
			accountId: accountId ?? OPENCLAW_DEFAULT_ACCOUNT_ID,
			dmPolicy,
			groupPolicy,
			allowFromCount: allowFrom.length,
		});

		const channelConfig: ChannelConfig = {
			channelType: 'telegram',
			botToken,
			accountId,
			dmPolicy,
			allowFrom,
			groupPolicy,
		};

		return { response: channelConfig };
	}

	methods = {
		loadOptions: {
			async getAllowFromOptions(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const accountId = normalizeOpenClawAccountId(this.getCurrentNodeParameter('accountId'));
				return getTelegramAllowFromOptions(accountId);
			},
		},
	};
}
