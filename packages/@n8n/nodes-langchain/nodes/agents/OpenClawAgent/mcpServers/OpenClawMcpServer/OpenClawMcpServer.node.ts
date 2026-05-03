import {
	NodeConnectionTypes,
	NodeOperationError,
	jsonParse,
	type IDataObject,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import type { McpServerConfig } from '../../V2/OpenClawAgentV2.node';

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseHeaders(
	value: unknown,
	ctx: ISupplyDataFunctions,
	itemIndex: number,
): Record<string, string | number | boolean> | undefined {
	if (value === undefined || value === null || value === '') {
		return undefined;
	}

	let rawHeaders: unknown = value;
	if (typeof value === 'string') {
		const trimmed = value.trim();
		if (!trimmed || trimmed === '{}') {
			return undefined;
		}
		rawHeaders = jsonParse<unknown>(trimmed, { acceptJSObject: true, repairJSON: true });
	}

	if (!isObject(rawHeaders)) {
		throw new NodeOperationError(ctx.getNode(), 'Headers must be a JSON object', { itemIndex });
	}

	const headers: Record<string, string | number | boolean> = {};
	for (const [key, headerValue] of Object.entries(rawHeaders)) {
		if (
			typeof headerValue !== 'string' &&
			typeof headerValue !== 'number' &&
			typeof headerValue !== 'boolean'
		) {
			throw new NodeOperationError(
				ctx.getNode(),
				`Header "${key}" must be a string, number, or boolean`,
				{ itemIndex },
			);
		}
		headers[key] = headerValue;
	}

	return Object.keys(headers).length > 0 ? headers : undefined;
}

function assertHttpUrl(value: string, ctx: ISupplyDataFunctions, itemIndex: number): void {
	try {
		const parsed = new URL(value);
		if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
			return;
		}
	} catch {
		// handled below
	}
	throw new NodeOperationError(ctx.getNode(), 'Endpoint URL must be a valid HTTP or HTTPS URL', {
		itemIndex,
	});
}

export class OpenClawMcpServer implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenClaw MCP Server',
		name: 'openClawMcpServer',
		icon: 'file:openclaw-mcp-server.svg',
		iconColor: 'blue',
		group: ['transform'],
		version: 1,
		description: 'Provides MCP server configuration to an OpenClaw AI Agent',
		defaults: {
			name: 'OpenClaw MCP Server',
		},
		codex: {
			alias: ['OpenClaw', 'MCP', 'MCP Server', 'Model Context Protocol', 'BrowserOS'],
			categories: ['AI'],
			subcategories: {
				AI: ['Model Context Protocol', 'Tools'],
			},
			resources: {
				primaryDocumentation: [
					{
						url: 'https://docs.openclaw.ai/cli/mcp',
					},
				],
			},
		},
		inputs: [],
		outputs: [{ type: NodeConnectionTypes.AiTool, displayName: 'MCP Server' }],
		outputNames: ['MCP Server'],
		properties: [
			{
				displayName:
					'Connect this node to the MCP Server input of an OpenClaw AI Agent to sync a named MCP endpoint into OpenClaw config.',
				name: 'mcpServerNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Server Name',
				name: 'serverName',
				type: 'string',
				required: true,
				default: 'browseros',
				description: 'Name to use under OpenClaw mcp.servers',
			},
			{
				displayName: 'Endpoint URL',
				name: 'endpointUrl',
				type: 'string',
				required: true,
				default: 'http://127.0.0.1:9001/mcp',
				placeholder: 'e.g. http://127.0.0.1:9001/mcp',
				description: 'HTTP or HTTPS URL of the MCP server endpoint',
			},
			{
				displayName: 'Transport',
				name: 'transport',
				type: 'options',
				default: 'streamable-http',
				noDataExpression: true,
				description: 'HTTP transport OpenClaw should use for this MCP server',
				options: [
					{
						name: 'OpenClaw Default',
						value: '',
						description: 'Do not write a transport value',
					},
					{
						name: 'Server-Sent Events (SSE)',
						value: 'sse',
						description: 'Use SSE transport',
					},
					{
						name: 'Streamable HTTP',
						value: 'streamable-http',
						description: 'Use Streamable HTTP transport',
					},
				],
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Connection Timeout',
						name: 'connectionTimeoutMs',
						type: 'number',
						default: 30000,
						description: 'Time in milliseconds to wait while connecting to the MCP server',
						typeOptions: { minValue: 1 },
					},
					{
						displayName: 'Headers',
						name: 'headers',
						type: 'json',
						default: '{}',
						description: 'Optional HTTP headers as a JSON object',
						typeOptions: { rows: 4 },
					},
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const node = this.getNode();
		console.log('[OpenClawMcpServer] supplyData ENTRY', {
			nodeName: node.name,
			nodeId: node.id,
			nodeType: node.type,
			itemIndex,
			parameterNames: Object.keys(node.parameters ?? {}),
		});

		const serverName = (this.getNodeParameter('serverName', itemIndex, '') as string).trim();
		const endpointUrl = (this.getNodeParameter('endpointUrl', itemIndex, '') as string).trim();
		const transport = this.getNodeParameter('transport', itemIndex, 'streamable-http') as
			| ''
			| 'sse'
			| 'streamable-http';
		const options = this.getNodeParameter('options', itemIndex, {}) as IDataObject;
		const headers = parseHeaders(options.headers, this, itemIndex);
		const rawConnectionTimeoutMs = options.connectionTimeoutMs;
		const connectionTimeoutMs =
			typeof rawConnectionTimeoutMs === 'number' &&
			Number.isFinite(rawConnectionTimeoutMs) &&
			rawConnectionTimeoutMs > 0
				? Math.floor(rawConnectionTimeoutMs)
				: undefined;

		console.log('[OpenClawMcpServer] supplyData called', {
			itemIndex,
			serverName,
			endpointUrl,
			transport: transport || '(openclaw-default)',
			headerKeys: headers ? Object.keys(headers) : [],
			connectionTimeoutMs,
		});

		if (!serverName) {
			throw new NodeOperationError(this.getNode(), 'Server Name must not be empty', { itemIndex });
		}
		if (!endpointUrl) {
			throw new NodeOperationError(this.getNode(), 'Endpoint URL must not be empty', { itemIndex });
		}
		assertHttpUrl(endpointUrl, this, itemIndex);

		const mcpServerConfig: McpServerConfig = {
			mcpServerSource: 'openclaw',
			serverName,
			url: endpointUrl,
			transport: transport || undefined,
			headers,
			connectionTimeoutMs,
		};

		console.log('[OpenClawMcpServer] returning MCP Server config', {
			serverName: mcpServerConfig.serverName,
			url: mcpServerConfig.url,
			transport: mcpServerConfig.transport ?? '(openclaw-default)',
			headerKeys: mcpServerConfig.headers ? Object.keys(mcpServerConfig.headers) : [],
			connectionTimeoutMs: mcpServerConfig.connectionTimeoutMs,
		});

		return { response: mcpServerConfig };
	}
}
