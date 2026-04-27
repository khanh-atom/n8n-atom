import { BaseChatModel } from '@langchain/core/language_models/chat_models';
import type { BaseMessage } from '@langchain/core/messages';
import { AIMessage, HumanMessage, SystemMessage } from '@langchain/core/messages';
import type { ChatResult } from '@langchain/core/outputs';
import type { CallbackManagerForLLMRun } from '@langchain/core/callbacks/manager';
import type { BindToolsInput } from '@langchain/core/language_models/chat_models';
import {
	ApplicationError,
	NodeConnectionTypes,
	type INodeType,
	type INodeTypeDescription,
	type ISupplyDataFunctions,
	type SupplyData,
} from 'n8n-workflow';

import { getConnectionHintNoticeField } from '@utils/sharedFields';

import { N8nLlmTracing } from '../N8nLlmTracing';
import { spawn } from 'child_process';
import { existsSync, statSync } from 'fs';

interface OpenCodeCliFields {
	model: string;
	binaryPath: string;
	workingDirectory: string;
}

interface ParsedToolCall {
	id: string;
	name: string;
	args: Record<string, unknown>;
}

interface ParsedJsonEventResult {
	assistantText: string;
	errorMessage: string;
}

const TOOL_CALL_SYSTEM_PROMPT = `You have access to the following tools. When you need to call a tool, respond ONLY with a JSON block in this exact format (no other text before or after):

\`\`\`tool_calls
[{"id": "call_1", "name": "tool_name", "args": {"param": "value"}}]
\`\`\`

When you do NOT need to call a tool, respond normally with text. Never mix tool calls and text in the same response.

Available tools:
`;

/**
 * Custom LangChain chat model that wraps the OpenCode CLI binary.
 * Uses `opencode run --format json` for non-interactive execution.
 * Supports tool calling by injecting tool schemas into the prompt
 * and parsing structured JSON responses for tool calls.
 *
 * OpenCode CLI JSON output event types (verified empirically):
 * - {"type":"step_start", "part":{"type":"step-start",...}}
 * - {"type":"text", "part":{"type":"text","text":"...the response...",...}}
 * - {"type":"step_finish", "part":{"type":"step-finish","reason":"stop","cost":...,"tokens":{...}}}
 */
class ChatOpenCodeCLI extends BaseChatModel {
	model: string;

	binaryPath: string;

	workingDirectory: string;

	boundTools: BindToolsInput[] = [];

	constructor(fields: OpenCodeCliFields) {
		super({});
		this.model = fields.model;
		this.binaryPath = fields.binaryPath;
		this.workingDirectory = fields.workingDirectory;
	}

	_llmType(): string {
		return 'opencode-cli';
	}

	override bindTools(tools: BindToolsInput[], kwargs?: Partial<this['ParsedCallOptions']>) {
		console.log('[LmChatOpenCodeCli] bindTools called, tool count:', tools.length);
		const clone = new ChatOpenCodeCLI({
			model: this.model,
			binaryPath: this.binaryPath,
			workingDirectory: this.workingDirectory,
		});
		clone.boundTools = tools;
		clone.callbacks = this.callbacks;
		if (kwargs) {
			return (
				clone as unknown as {
					bind: (kwargs: Record<string, unknown>) => ChatOpenCodeCLI;
				}
			).bind(kwargs as Record<string, unknown>);
		}
		return clone;
	}

	async _generate(
		messages: BaseMessage[],
		_options: this['ParsedCallOptions'],
		_runManager?: CallbackManagerForLLMRun,
	): Promise<ChatResult> {
		console.log('[LmChatOpenCodeCli] _generate called', {
			messageCount: messages.length,
			boundToolCount: this.boundTools.length,
			model: this.model,
		});

		// If tools are bound, inject tool schemas into a system message
		const processedMessages = [...messages];
		if (this.boundTools.length > 0) {
			const toolDescriptions = this.boundTools
				.map((tool) => {
					const t = tool as Record<string, unknown>;
					const name = (t.name as string) ?? '';
					const description = (t.description as string) ?? '';
					const schema = t.parameters ?? t.schema ?? {};
					return `- ${name}: ${description}\n  Parameters: ${JSON.stringify(schema)}`;
				})
				.join('\n\n');

			const systemPrompt = TOOL_CALL_SYSTEM_PROMPT + toolDescriptions;
			processedMessages.unshift(new SystemMessage(systemPrompt));
			console.log(
				'[LmChatOpenCodeCli] injected tool system prompt, tool count:',
				this.boundTools.length,
			);
		}

		// Build prompt from messages
		const prompt = processedMessages
			.map((m) => {
				const content = typeof m.content === 'string' ? m.content : JSON.stringify(m.content);
				if (m instanceof SystemMessage) return `[system]: ${content}`;
				if (m instanceof HumanMessage) return `[user]: ${content}`;
				if (m instanceof AIMessage) return `[assistant]: ${content}`;
				return `[${m._getType()}]: ${content}`;
			})
			.join('\n\n');

		console.log('[LmChatOpenCodeCli] prompt built, length:', prompt.length);

		// Execute opencode CLI
		const rawResponse = await this.executeOpenCodeCli(prompt);

		console.log('[LmChatOpenCodeCli] raw response received, length:', rawResponse.length);

		// Check for tool calls in response
		if (this.boundTools.length > 0) {
			const toolCalls = this.extractToolCalls(rawResponse);
			if (toolCalls.length > 0) {
				console.log('[LmChatOpenCodeCli] extracted tool calls:', toolCalls.length);
				const aiMessage = new AIMessage({
					content: '',
					tool_calls: toolCalls.map((tc) => ({
						id: tc.id,
						name: tc.name,
						args: tc.args,
						type: 'tool_call' as const,
					})),
				});

				return {
					generations: [{ message: aiMessage, text: '' }],
				};
			}
		}

		// Normal text response
		console.log('[LmChatOpenCodeCli] returning text response');
		const aiMessage = new AIMessage({ content: rawResponse });
		return {
			generations: [{ message: aiMessage, text: rawResponse }],
		};
	}

	private extractToolCalls(text: string): ParsedToolCall[] {
		// Look for tool_calls JSON block
		const toolCallRegex = /```tool_calls\s*\n([\s\S]*?)\n```/;
		const match = toolCallRegex.exec(text);
		if (!match) return [];

		try {
			const parsed = JSON.parse(match[1]) as Array<{
				id?: string;
				name: string;
				args: Record<string, unknown>;
			}>;
			if (!Array.isArray(parsed)) return [];

			console.log('[LmChatOpenCodeCli] parsed tool calls from response:', parsed.length);
			return parsed.map((tc, i) => ({
				id: tc.id ?? `call_${i}`,
				name: tc.name,
				args: tc.args ?? {},
			}));
		} catch {
			console.log('[LmChatOpenCodeCli] failed to parse tool calls JSON block');
			return [];
		}
	}

	private async executeOpenCodeCli(prompt: string): Promise<string> {
		// Build args: opencode run --format json [--model <provider/model>] <prompt>
		// The prompt is passed as a positional argument to `opencode run`
		// We use --format json to get structured JSONL output
		const args = ['run', '--format', 'json'];

		if (this.model && this.model !== 'auto') {
			args.push('--model', this.model);
		}

		// The prompt is passed via stdin-like mechanism — actually as positional arg
		// But since prompts can be very long, we pass it as a positional argument
		args.push(prompt);

		const cwd = this.workingDirectory?.trim() || undefined;

		console.log('[LmChatOpenCodeCli] spawning opencode run', {
			binaryPath: this.binaryPath,
			args: args.map((a, i) => (i === args.length - 1 ? `<prompt len=${a.length}>` : a)),
			model: this.model,
			cwd,
		});

		return await new Promise<string>((resolve, reject) => {
			const child = spawn(this.binaryPath, args, {
				cwd,
				stdio: ['pipe', 'pipe', 'pipe'],
				env: { ...process.env },
			});

			let stdout = '';
			let stderr = '';

			child.stdout.on('data', (data: Buffer) => {
				stdout += data.toString();
			});

			child.stderr.on('data', (data: Buffer) => {
				stderr += data.toString();
			});

			child.on('error', (err: Error) => {
				console.error('[LmChatOpenCodeCli] spawn error:', err.message);
				reject(
					new Error(
						`Failed to spawn opencode: ${err.message}. Make sure OpenCode CLI is installed (brew install opencode-ai/tap/opencode or curl -fsSL https://opencode.ai/install | bash) and accessible. Working directory: ${cwd ?? '<default>'}`,
					),
				);
			});

			child.on('close', (code: number | null) => {
				console.log('[LmChatOpenCodeCli] opencode run exited', {
					code,
					stdoutLength: stdout.length,
					stderrLength: stderr.length,
				});

				// Parse the JSON event output — even on non-zero exit, stdout may
				// contain useful events (e.g. error messages from the provider)
				const parseResult = this.parseJsonEventOutput(stdout);

				if (parseResult.assistantText) {
					console.log(
						'[LmChatOpenCodeCli] parsed assistant content, length:',
						parseResult.assistantText.length,
					);
					resolve(parseResult.assistantText);
					return;
				}

				// No assistant response — build a meaningful error from available info
				if (parseResult.errorMessage) {
					console.error('[LmChatOpenCodeCli] opencode returned error:', parseResult.errorMessage);
					reject(new Error(`OpenCode CLI error: ${parseResult.errorMessage}`));
					return;
				}

				if (code !== 0) {
					const stderrMsg = stderr.trim();
					const errorMsg = stderrMsg || `opencode run exited with code ${code}`;
					console.error('[LmChatOpenCodeCli] opencode run failed with code', code, ':', errorMsg);
					reject(new Error(errorMsg));
					return;
				}

				console.error(
					'[LmChatOpenCodeCli] no assistant response parsed from output, stdout preview:',
					stdout.substring(0, 500),
				);
				reject(new Error('No assistant response received from opencode run'));
			});

			// Close stdin immediately — opencode run takes the prompt as a positional arg
			if (child.stdin) {
				child.stdin.end();
			}
		});
	}

	/**
	 * Parse JSON event output from `opencode run --format json`.
	 *
	 * Actual event types from opencode run --format json (verified empirically):
	 * - {"type":"step_start","part":{"type":"step-start",...}}
	 * - {"type":"text","part":{"type":"text","text":"...the response...",...}}
	 * - {"type":"step_finish","part":{"type":"step-finish","reason":"stop","cost":...,"tokens":{...}}}
	 *
	 * Returns both assistant text and any error messages found.
	 */
	private parseJsonEventOutput(output: string): ParsedJsonEventResult {
		const lines = output.split('\n').filter((line) => line.trim());
		const assistantParts: string[] = [];
		const errorParts: string[] = [];

		console.log('[LmChatOpenCodeCli] parsing JSON event output, line count:', lines.length);

		for (const line of lines) {
			try {
				const parsed = JSON.parse(line) as Record<string, unknown>;
				const eventType = parsed.type as string | undefined;

				console.log(
					'[LmChatOpenCodeCli] JSON event:',
					eventType,
					'| keys:',
					Object.keys(parsed).join(','),
				);

				// "text" event — contains assistant response text in part.text
				if (eventType === 'text') {
					const part = parsed.part as Record<string, unknown> | undefined;
					if (part?.type === 'text' && typeof part.text === 'string') {
						console.log(
							'[LmChatOpenCodeCli] found text event, text length:',
							(part.text as string).length,
						);
						assistantParts.push(part.text as string);
					}
				}

				// "message" event — fallback for alternative output formats
				if (eventType === 'message') {
					const part = parsed.part as Record<string, unknown> | undefined;
					if (part && typeof part.text === 'string') {
						assistantParts.push(part.text as string);
					}
					// Also handle role-based messages
					if (parsed.role === 'assistant' && typeof parsed.content === 'string') {
						assistantParts.push(parsed.content as string);
					}
				}

				// "assistant" event — compatibility fallback
				if (eventType === 'assistant') {
					const message = parsed.message as Record<string, unknown> | undefined;
					if (message?.content) {
						if (Array.isArray(message.content)) {
							for (const c of message.content as Array<Record<string, unknown>>) {
								if (c.type === 'text' && typeof c.text === 'string') {
									assistantParts.push(c.text as string);
								}
							}
						} else if (typeof message.content === 'string') {
							assistantParts.push(message.content);
						}
					}
				}

				// Error events
				if (eventType === 'error') {
					const errMsg =
						typeof parsed.message === 'string'
							? parsed.message
							: typeof parsed.error === 'string'
								? parsed.error
								: undefined;
					if (errMsg) {
						console.error('[LmChatOpenCodeCli] error event received:', errMsg);
						errorParts.push(errMsg);
					}
				}

				// step_finish with error info
				if (eventType === 'step_finish') {
					const part = parsed.part as Record<string, unknown> | undefined;
					if (part) {
						console.log(
							'[LmChatOpenCodeCli] step_finish event, reason:',
							part.reason,
							'cost:',
							part.cost,
						);
						if (part.reason === 'error' && typeof part.error === 'string') {
							errorParts.push(part.error as string);
						}
					}
				}
			} catch {
				// Skip non-JSON lines (e.g. progress output, banners)
			}
		}

		return {
			assistantText: assistantParts.join(''),
			errorMessage: errorParts.join('; '),
		};
	}
}

export class LmChatOpenCodeCli implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'OpenCode CLI Chat Model',

		name: 'lmChatOpenCodeCli',
		icon: 'file:openCodeCli.svg',
		group: ['transform'],
		version: [1],
		description:
			'Chat model powered by the OpenCode CLI. Requires opencode to be installed locally (brew install opencode-ai/tap/opencode or curl -fsSL https://opencode.ai/install | bash).',
		defaults: {
			name: 'OpenCode CLI Chat Model',
		},
		codex: {
			categories: ['AI'],
			subcategories: {
				AI: ['Language Models', 'Root Nodes'],
				'Language Models': ['Chat Models (Recommended)'],
			},
			resources: {},
		},

		inputs: [],

		outputs: [NodeConnectionTypes.AiLanguageModel],
		outputNames: ['Model'],
		properties: [
			getConnectionHintNoticeField([NodeConnectionTypes.AiChain, NodeConnectionTypes.AiAgent]),
			{
				displayName: 'Model',
				name: 'model',
				type: 'options',
				description:
					'The model to use via opencode CLI. Format: provider/model (e.g. anthropic/claude-sonnet-4-20250514). Select "Auto" to use the default model configured in opencode.',
				// eslint-disable-next-line n8n-nodes-base/node-param-options-type-unsorted-items
				options: [
					{ name: 'Auto (Default)', value: 'auto' },
					// OpenCode built-in models
					{ name: 'OpenCode Big Pickle', value: 'opencode/big-pickle' },
					{ name: 'OpenCode GPT-5 Nano', value: 'opencode/gpt-5-nano' },
					{ name: 'OpenCode Hy3 Preview Free', value: 'opencode/hy3-preview-free' },
					// Anthropic models
					{ name: 'Claude Sonnet 4 (Anthropic)', value: 'anthropic/claude-sonnet-4-20250514' },
					{ name: 'Claude Opus 4 (Anthropic)', value: 'anthropic/claude-opus-4-20250918' },
					{
						name: 'Claude 3.5 Sonnet (Anthropic)',
						value: 'anthropic/claude-3-5-sonnet-20241022',
					},
					// OpenAI models
					{ name: 'GPT-4o (OpenAI)', value: 'openai/gpt-4o' },
					{ name: 'GPT-4o Mini (OpenAI)', value: 'openai/gpt-4o-mini' },
					{ name: 'o3 (OpenAI)', value: 'openai/o3' },
					{ name: 'o3 Mini (OpenAI)', value: 'openai/o3-mini' },
					// Google models
					{ name: 'Gemini 2.5 Pro (Google)', value: 'google/gemini-2.5-pro' },
					{ name: 'Gemini 2.5 Flash (Google)', value: 'google/gemini-2.5-flash' },
					{ name: 'Gemini 2.0 Flash (Google)', value: 'google/gemini-2.0-flash' },
					// xAI models
					{ name: 'Grok 3 (xAI)', value: 'xai/grok-3' },
					{ name: 'Grok 3 Mini (xAI)', value: 'xai/grok-3-mini' },
					// DeepSeek models
					{ name: 'DeepSeek Chat (DeepSeek)', value: 'deepseek/deepseek-chat' },
					{ name: 'DeepSeek Reasoner (DeepSeek)', value: 'deepseek/deepseek-reasoner' },
				],
				default: 'auto',
			},
			{
				displayName: 'Options',
				name: 'options',
				placeholder: 'Add Option',
				description: 'Additional options to configure',
				type: 'collection',
				default: {},
				options: [
					{
						displayName: 'Binary Path',
						name: 'binaryPath',
						default: 'opencode',
						description: 'Path to the opencode binary. Defaults to "opencode" (must be in PATH).',
						type: 'string',
					},
					{
						displayName: 'Working Directory',
						name: 'workingDirectory',
						default: '',
						description:
							'Working directory for the opencode process. Leave empty to use the default.',
						type: 'string',
					},
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const modelName = this.getNodeParameter('model', itemIndex) as string;

		const binaryPath = this.getNodeParameter('options.binaryPath', itemIndex, 'opencode') as string;
		const rawWorkingDirectory = this.getNodeParameter('options.workingDirectory', itemIndex, '', {
			rawExpressions: true,
		}) as string | undefined;
		const workingDirectory = this.getNodeParameter('options.workingDirectory', itemIndex, '') as
			| string
			| undefined;
		const normalizedWorkingDirectory = (workingDirectory ?? '').trim();
		const rawWorkingDirectoryValue = rawWorkingDirectory ?? '';
		const isWorkingDirectoryExpression =
			rawWorkingDirectoryValue.startsWith('=') ||
			rawWorkingDirectoryValue.includes('{{') ||
			rawWorkingDirectoryValue.includes('$workspace');

		console.log('[LmChatOpenCodeCli] resolved OpenCode CLI options', {
			itemIndex,
			modelName,
			binaryPath,
			rawWorkingDirectory,
			workingDirectory: normalizedWorkingDirectory,
		});

		if (isWorkingDirectoryExpression && !normalizedWorkingDirectory) {
			throw new ApplicationError(
				`OpenCode CLI working directory expression resolved to an empty value: ${rawWorkingDirectoryValue}`,
			);
		}

		if (
			normalizedWorkingDirectory.includes('{{') ||
			normalizedWorkingDirectory.includes('$workspace')
		) {
			throw new ApplicationError(
				`OpenCode CLI working directory was not resolved before execution: ${normalizedWorkingDirectory}`,
			);
		}

		if (
			normalizedWorkingDirectory &&
			(!existsSync(normalizedWorkingDirectory) ||
				!statSync(normalizedWorkingDirectory).isDirectory())
		) {
			throw new ApplicationError(
				`OpenCode CLI working directory does not exist or is not a directory: ${normalizedWorkingDirectory}`,
			);
		}

		console.log('[LmChatOpenCodeCli] creating ChatOpenCodeCLI instance', {
			model: modelName,
			binaryPath,
			workingDirectory: normalizedWorkingDirectory,
		});

		const model = new ChatOpenCodeCLI({
			model: modelName,
			binaryPath,
			workingDirectory: normalizedWorkingDirectory,
		});

		model.callbacks = [new N8nLlmTracing(this)];

		return {
			response: model,
		};
	}
}
