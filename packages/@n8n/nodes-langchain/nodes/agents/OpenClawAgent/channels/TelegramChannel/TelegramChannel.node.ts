import {
	NodeConnectionTypes,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { ChannelConfig } from '../../V2/OpenClawAgentV2.node';

const TELEGRAM_CREDENTIAL_TYPE = 'telegramApi';

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
		const groupPolicy = this.getNodeParameter('groupPolicy', itemIndex, 'allowlist') as string;

		const channelConfig: ChannelConfig = {
			channelType: 'telegram',
			botToken,
			accountId,
			dmPolicy,
			groupPolicy,
		};

		return { response: channelConfig };
	}
}
