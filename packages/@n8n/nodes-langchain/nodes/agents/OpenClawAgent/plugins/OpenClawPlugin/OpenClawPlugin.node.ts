import {
	NodeConnectionTypes,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { PluginConfig } from '../../V2/OpenClawAgentV2.node';

/**
 * OpenClaw Plugin sub-node for OpenClaw Agent.
 *
 * This node provides a plugin directory path to the OpenClaw AI Agent
 * via the AiTool connection. It supplies the path to a directory that
 * contains OpenClaw plugins (directories with `openclaw.plugin.json`
 * or `package.json` manifests).
 *
 * The agent uses these paths to discover and load plugins, passing
 * them to the CLI via the OPENCLAW_PLUGIN_PATHS environment variable
 * and syncing them to openclaw.json.
 *
 * Supports expressions like `={{ $workspace.__dirPath }}` to
 * dynamically resolve to the workflow's working directory.
 */
export class OpenClawPlugin implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenClaw Plugin',
		name: 'openClawPlugin',
		icon: 'file:openclaw-plugin.svg',
		iconColor: 'purple',
		group: ['transform'],
		version: 1,
		description: 'Provides a plugin directory path to an OpenClaw AI Agent',
		defaults: {
			name: 'OpenClaw Plugin',
		},
		codex: {
			alias: ['OpenClaw', 'Plugin', 'Extension'],
			categories: ['AI'],
			subcategories: {
				AI: ['Other'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://docs.openclaw.ai/plugins',
					},
				],
			},
		},
		inputs: [],
		outputs: [NodeConnectionTypes.AiTool],
		outputNames: ['Plugin'],
		// No credentials needed — plugins are loaded from the filesystem
		properties: [
			{
				displayName:
					'Connect this node to an OpenClaw AI Agent to provide a plugin directory path. Use expressions like {{ $workspace.__dirPath }} to reference the workflow directory.',
				name: 'pluginNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Plugin Path',
				name: 'pluginPath',
				type: 'string',
				required: true,
				default: '={{ $workspace.__dirPath }}',
				description:
					'Directory path containing OpenClaw plugins (folders with openclaw.plugin.json or package.json). Supports expressions.',
			},
			{
				displayName: 'Plugin Name',
				name: 'pluginName',
				type: 'string',
				default: '',
				description: 'Optional friendly name for this plugin source (used in logs)',
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const pluginPath = (this.getNodeParameter('pluginPath', itemIndex, '') as string).trim();
		const pluginName =
			(this.getNodeParameter('pluginName', itemIndex, '') as string).trim() || undefined;

		console.log('[OpenClawPlugin] supplyData called', {
			itemIndex,
			pluginPath,
			pluginName: pluginName ?? '(unnamed)',
		});

		if (!pluginPath) {
			console.log('[OpenClawPlugin] WARNING: pluginPath is empty, returning empty config', {
				itemIndex,
			});
		}

		const pluginConfig: PluginConfig = {
			pluginPath,
			pluginName,
		};

		console.log('[OpenClawPlugin] returning plugin config', {
			pluginPath: pluginConfig.pluginPath,
			pluginName: pluginConfig.pluginName ?? '(unnamed)',
		});

		return { response: pluginConfig };
	}
}
