import {
	NodeConnectionTypes,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { ChannelConfig } from '../../V2/OpenClawAgentV2.node';

const WHATSAPP_CREDENTIAL_TYPE = 'whatsAppBusinessApi';

export class WhatsAppChannel implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'WhatsApp Channel',
		name: 'openClawWhatsAppChannel',
		icon: 'file:whatsapp-channel.svg',
		iconColor: 'green',
		group: ['transform'],
		version: 1,
		description: 'Provides WhatsApp channel configuration to an OpenClaw AI Agent',
		defaults: {
			name: 'WhatsApp Channel',
		},
		codex: {
			alias: ['WhatsApp', 'Channel', 'OpenClaw'],
			categories: ['AI'],
			subcategories: {
				AI: ['Other'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://docs.openclaw.ai/channels/whatsapp',
					},
				],
			},
		},
		inputs: [],
		outputs: [NodeConnectionTypes.AiChannel],
		outputNames: ['Channel'],
		credentials: [
			{
				name: WHATSAPP_CREDENTIAL_TYPE,
				required: true,
			},
		],
		properties: [
			{
				displayName:
					'Connect this node to an OpenClaw AI Agent to provide WhatsApp channel configuration.',
				name: 'whatsappNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Phone Number ID',
				name: 'phoneNumberId',
				type: 'string',
				default: '',
				required: true,
				description: 'WhatsApp Business phone number ID',
			},
			{
				displayName: 'Account ID',
				name: 'accountId',
				type: 'string',
				default: '',
				description:
					'Optional OpenClaw account ID for multi-account setups. Leave empty for the default account.',
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const credentials = await this.getCredentials<{ accessToken?: string }>(
			WHATSAPP_CREDENTIAL_TYPE,
			itemIndex,
		);

		const accessToken = credentials.accessToken?.trim();
		const phoneNumberId = (this.getNodeParameter('phoneNumberId', itemIndex, '') as string).trim();
		const accountId =
			(this.getNodeParameter('accountId', itemIndex, '') as string).trim() || undefined;

		const channelConfig: ChannelConfig = {
			channelType: 'whatsapp',
			accessToken,
			phoneNumberId,
			accountId,
		};

		return { response: channelConfig };
	}
}
