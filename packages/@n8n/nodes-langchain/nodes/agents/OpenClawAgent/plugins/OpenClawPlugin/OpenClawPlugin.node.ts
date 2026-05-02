import { existsSync, readFileSync } from 'fs';
import { join } from 'path';

import {
	NodeConnectionTypes,
	NodeOperationError,
	type IDataObject,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { PluginConfig } from '../../V2/OpenClawAgentV2.node';

const PLUGIN_MANIFEST_FILENAME = 'openclaw.plugin.json';

/**
 * Attempt to read and parse an openclaw.plugin.json manifest from a directory.
 * Returns the parsed manifest object or undefined if not found / invalid.
 */
function loadLocalPluginManifest(dirPath: string): PluginConfig['pluginManifest'] | undefined {
	const manifestPath = join(dirPath, PLUGIN_MANIFEST_FILENAME);
	console.log('[OpenClawPlugin] Scanning for local manifest', {
		dirPath,
		manifestPath,
		exists: existsSync(manifestPath),
	});

	if (!existsSync(manifestPath)) {
		console.log('[OpenClawPlugin] No openclaw.plugin.json found at path', {
			dirPath,
			manifestPath,
		});
		return undefined;
	}

	try {
		const raw = readFileSync(manifestPath, 'utf8').trim();
		if (!raw) {
			console.log('[OpenClawPlugin] Manifest file is empty', { manifestPath });
			return undefined;
		}

		const parsed = JSON.parse(raw) as IDataObject;
		if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
			console.log('[OpenClawPlugin] Manifest is not a valid JSON object', {
				manifestPath,
				type: typeof parsed,
			});
			return undefined;
		}

		const manifest: PluginConfig['pluginManifest'] = {
			id: typeof parsed.id === 'string' ? parsed.id : undefined,
			name: typeof parsed.name === 'string' ? parsed.name : undefined,
			description: typeof parsed.description === 'string' ? parsed.description : undefined,
			version: typeof parsed.version === 'string' ? parsed.version : undefined,
			providers: Array.isArray(parsed.providers)
				? (parsed.providers as unknown[]).filter((p): p is string => typeof p === 'string')
				: undefined,
			channels: Array.isArray(parsed.channels)
				? (parsed.channels as unknown[]).filter((c): c is string => typeof c === 'string')
				: undefined,
		};

		console.log('[OpenClawPlugin] Successfully loaded local manifest', {
			manifestPath,
			id: manifest.id,
			name: manifest.name,
			version: manifest.version,
			providerCount: manifest.providers?.length ?? 0,
			channelCount: manifest.channels?.length ?? 0,
		});

		return manifest;
	} catch (error) {
		console.log('[OpenClawPlugin] Failed to parse manifest file', {
			manifestPath,
			error: error instanceof Error ? error.message : String(error),
		});
		return undefined;
	}
}

/**
 * OpenClaw Plugin sub-node for OpenClaw Agent.
 *
 * This node provides plugin configuration to the OpenClaw AI Agent
 * via the AiTool connection. It supports two plugin sources:
 *
 * - **Local**: scans a directory path for `openclaw.plugin.json` and
 *   loads manifest info (id, name, providers, channels, etc.).
 *   Uses `$workspace.__dirPath` by default to scan the workflow directory.
 *
 * - **Cloud**: references a plugin from ClawHub by package name
 *   (e.g. "openai" or "@scope/pkg") and optional version.
 */
export class OpenClawPlugin implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenClaw Plugin',
		name: 'openClawPlugin',
		icon: 'file:openclaw-plugin.svg',
		iconColor: 'purple',
		group: ['transform'],
		version: 1,
		description: 'Provides plugin configuration to an OpenClaw AI Agent (local or ClawHub)',
		defaults: {
			name: 'OpenClaw Plugin',
		},
		codex: {
			alias: ['OpenClaw', 'Plugin', 'Extension', 'ClawHub'],
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
		// No credentials needed — local plugins are filesystem-based, cloud uses public ClawHub
		properties: [
			{
				displayName:
					'Connect this node to an OpenClaw AI Agent to provide plugin configuration. Local plugins are scanned from a directory; Cloud plugins are loaded from ClawHub.',
				name: 'pluginNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Plugin Source',
				name: 'pluginSource',
				type: 'options',
				default: 'local',
				noDataExpression: true,
				description: 'Where to load the plugin from',
				options: [
					{
						name: 'Local',
						value: 'local',
						description: 'Scan a directory for openclaw.plugin.json and load plugin info from it',
					},
					{
						name: 'Cloud (ClawHub)',
						value: 'cloud',
						description: 'Load a plugin from the ClawHub marketplace',
					},
				],
			},
			// ── Local source fields ──
			{
				displayName: 'Plugin Directory',
				name: 'pluginDirectory',
				type: 'string',
				required: true,
				default: '={{ $workspace.__dirPath }}',
				description:
					'Directory path to scan for openclaw.plugin.json. Supports expressions like {{ $workspace.__dirPath }}.',
				displayOptions: {
					show: {
						pluginSource: ['local'],
					},
				},
			},
			// ── Cloud source fields ──
			{
				displayName: 'Plugin ID',
				name: 'pluginId',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'e.g. openai, @scope/my-plugin',
				description: 'ClawHub plugin package name',
				displayOptions: {
					show: {
						pluginSource: ['cloud'],
					},
				},
			},
			{
				displayName: 'Version',
				name: 'pluginVersion',
				type: 'string',
				default: '',
				placeholder: 'latest',
				description: 'ClawHub plugin version. Leave empty to use the latest available version.',
				displayOptions: {
					show: {
						pluginSource: ['cloud'],
					},
				},
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const pluginSource = this.getNodeParameter('pluginSource', itemIndex, 'local') as
			| 'local'
			| 'cloud';

		console.log('[OpenClawPlugin] supplyData called', {
			itemIndex,
			pluginSource,
		});

		if (pluginSource === 'local') {
			const pluginDirectory = (
				this.getNodeParameter('pluginDirectory', itemIndex, '') as string
			).trim();

			console.log('[OpenClawPlugin] Local source: scanning directory', {
				itemIndex,
				pluginDirectory,
			});

			if (!pluginDirectory) {
				throw new NodeOperationError(
					this.getNode(),
					'Plugin Directory must not be empty for local plugin source',
					{ itemIndex },
				);
			}

			// Scan the directory for openclaw.plugin.json
			const pluginManifest = loadLocalPluginManifest(pluginDirectory);

			const pluginConfig: PluginConfig = {
				pluginSource: 'local',
				pluginPath: pluginDirectory,
				pluginManifest,
			};

			console.log('[OpenClawPlugin] Returning local plugin config', {
				pluginPath: pluginConfig.pluginPath,
				hasManifest: !!pluginManifest,
				manifestId: pluginManifest?.id,
				manifestName: pluginManifest?.name,
				manifestVersion: pluginManifest?.version,
			});

			return { response: pluginConfig };
		}

		// Cloud source
		const pluginId = (this.getNodeParameter('pluginId', itemIndex, '') as string).trim();
		const pluginVersion =
			(this.getNodeParameter('pluginVersion', itemIndex, '') as string).trim() || undefined;

		console.log('[OpenClawPlugin] Cloud source: ClawHub plugin', {
			itemIndex,
			pluginId,
			pluginVersion: pluginVersion ?? '(latest)',
		});

		if (!pluginId) {
			throw new NodeOperationError(
				this.getNode(),
				'Plugin ID must not be empty for cloud plugin source',
				{ itemIndex },
			);
		}

		const pluginConfig: PluginConfig = {
			pluginSource: 'cloud',
			pluginId,
			pluginVersion,
		};

		console.log('[OpenClawPlugin] Returning cloud plugin config', {
			pluginId: pluginConfig.pluginId,
			pluginVersion: pluginConfig.pluginVersion ?? '(latest)',
		});

		return { response: pluginConfig };
	}
}
