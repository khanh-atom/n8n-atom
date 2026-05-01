import {
	NodeConnectionTypes,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { ModelConfig } from '../../V2/OpenClawAgentV2.node';

/**
 * OpenCode Free Chat Model sub-node for OpenClaw Agent.
 *
 * This node provides model configuration to the OpenClaw AI Agent
 * via the AiLanguageModel connection. It supplies free-tier OpenCode
 * models (no API key required) that the agent passes as --model to
 * the OpenClaw CLI.
 *
 * Unlike the standalone LmChatOpenCodeCli node (which wraps the CLI
 * as a LangChain BaseChatModel), this node simply supplies a model
 * identifier string — the OpenClaw agent handles CLI invocation.
 */
export class OpenCodeFreeModel implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenCode Free Chat Model',
		name: 'openClawOpenCodeFreeModel',
		icon: 'file:opencode-free-model.svg',
		iconColor: 'green',
		group: ['transform'],
		version: 1,
		description: 'Provides an OpenCode free-tier model to an OpenClaw AI Agent (no API key needed)',
		defaults: {
			name: 'OpenCode Free Chat Model',
		},
		codex: {
			alias: ['OpenCode', 'Free', 'Model', 'OpenClaw'],
			categories: ['AI'],
			subcategories: {
				AI: ['Language Models'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://opencode.ai',
					},
				],
			},
		},
		inputs: [],
		outputs: [NodeConnectionTypes.AiLanguageModel],
		outputNames: ['Model'],
		// No credentials — free models do not require API keys
		properties: [
			{
				displayName:
					'Connect this node to an OpenClaw AI Agent to provide a free OpenCode model. No API key is needed for these models.',
				name: 'openCodeFreeNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Model',
				name: 'model',
				type: 'options',
				default: 'opencode/big-pickle',
				description: 'Select a free OpenCode model to use with the OpenClaw agent',
				// eslint-disable-next-line n8n-nodes-base/node-param-options-type-unsorted-items
				options: [
					{ name: 'OpenCode Big Pickle', value: 'opencode/big-pickle' },
					{ name: 'OpenCode GPT-5 Nano', value: 'opencode/gpt-5-nano' },
					{ name: 'OpenCode Hy3 Preview Free', value: 'opencode/hy3-preview-free' },
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const modelId = this.getNodeParameter('model', itemIndex, 'opencode/big-pickle') as string;

		console.log('[OpenCodeFreeModel] supplyData called', {
			itemIndex,
			modelId,
		});

		const modelConfig: ModelConfig = {
			modelId,
			modelSource: 'opencode-free',
		};

		console.log('[OpenCodeFreeModel] returning model config', modelConfig);

		return { response: modelConfig };
	}
}
