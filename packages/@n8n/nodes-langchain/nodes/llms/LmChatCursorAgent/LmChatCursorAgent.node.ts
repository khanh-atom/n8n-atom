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

interface CursorAgentFields {
	model: string;
	binaryPath: string;
	workingDirectory: string;
}

interface ParsedToolCall {
	id: string;
	name: string;
	args: Record<string, unknown>;
}

const TOOL_CALL_SYSTEM_PROMPT = `You have access to the following tools. When you need to call a tool, respond ONLY with a JSON block in this exact format (no other text before or after):

\`\`\`tool_calls
[{"id": "call_1", "name": "tool_name", "args": {"param": "value"}}]
\`\`\`

When you do NOT need to call a tool, respond normally with text. Never mix tool calls and text in the same response.

Available tools:
`;

/**
 * Custom LangChain chat model that wraps the cursor-agent CLI binary.
 * Supports tool calling by injecting tool schemas into the prompt
 * and parsing structured JSON responses for tool calls.
 */
class ChatCursorAgentCLI extends BaseChatModel {
	model: string;

	binaryPath: string;

	workingDirectory: string;

	boundTools: BindToolsInput[] = [];

	constructor(fields: CursorAgentFields) {
		super({});
		this.model = fields.model;
		this.binaryPath = fields.binaryPath;
		this.workingDirectory = fields.workingDirectory;
	}

	_llmType(): string {
		return 'cursor-agent-cli';
	}

	override bindTools(tools: BindToolsInput[], kwargs?: Partial<this['ParsedCallOptions']>) {
		const clone = new ChatCursorAgentCLI({
			model: this.model,
			binaryPath: this.binaryPath,
			workingDirectory: this.workingDirectory,
		});
		clone.boundTools = tools;
		clone.callbacks = this.callbacks;
		if (kwargs) {
			return (
				clone as unknown as {
					bind: (kwargs: Record<string, unknown>) => ChatCursorAgentCLI;
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

		// Execute cursor-agent CLI
		const rawResponse = await this.executeCursorAgent(prompt);

		// Check for tool calls in response
		if (this.boundTools.length > 0) {
			const toolCalls = this.extractToolCalls(rawResponse);
			if (toolCalls.length > 0) {
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

			return parsed.map((tc, i) => ({
				id: tc.id ?? `call_${i}`,
				name: tc.name,
				args: tc.args ?? {},
			}));
		} catch {
			return [];
		}
	}

	private async executeCursorAgent(prompt: string): Promise<string> {
		const args = ['-p', '--output-format=stream-json', '--trust'];
		if (this.model && this.model !== 'auto') {
			args.push('--model', this.model);
		}
		const cwd = this.workingDirectory?.trim() || undefined;

		console.log('[LmChatCursorAgent] spawning cursor-agent', {
			binaryPath: this.binaryPath,
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
				reject(
					new Error(
						`Failed to spawn cursor-agent: ${err.message}. Make sure cursor-agent CLI is installed and accessible. Working directory: ${cwd ?? '<default>'}`,
					),
				);
			});

			child.on('close', (code: number | null) => {
				if (code !== 0 && !stdout) {
					const errorMsg = stderr.trim() || `cursor-agent exited with code ${code}`;
					reject(new Error(errorMsg));
					return;
				}

				const assistantContent = this.parseStreamJsonOutput(stdout);

				if (!assistantContent) {
					reject(new Error('No assistant response received from cursor-agent'));
					return;
				}

				resolve(assistantContent);
			});

			if (child.stdin) {
				child.stdin.write(prompt);
				child.stdin.end();
			}
		});
	}

	private parseStreamJsonOutput(output: string): string {
		const lines = output.split('\n').filter((line) => line.trim());
		const assistantParts: string[] = [];

		for (const line of lines) {
			try {
				const parsed = JSON.parse(line) as {
					type?: string;
					message?: {
						content?: Array<{ type?: string; text?: string }>;
					};
					text?: string;
				};

				if (parsed.type === 'assistant' && parsed.message?.content) {
					for (const item of parsed.message.content) {
						if (item.type === 'text' && item.text) {
							assistantParts.push(item.text);
						}
					}
				}
			} catch {
				// Skip non-JSON lines
			}
		}

		return assistantParts.join('');
	}
}

export class LmChatCursorAgent implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Cursor Agent CLI Chat Model',

		name: 'lmChatCursorAgent',
		icon: 'file:cursorAgent.svg',
		group: ['transform'],
		version: [1],
		description:
			'Chat model powered by the Cursor Agent CLI. Requires cursor-agent to be installed locally.',
		defaults: {
			name: 'Cursor Agent CLI Chat Model',
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
				description: 'The model to use via cursor-agent CLI',
				// eslint-disable-next-line n8n-nodes-base/node-param-options-type-unsorted-items
				options: [
					{ name: 'Auto', value: 'auto' },
					// Composer models
					{ name: 'Composer 2 Fast', value: 'composer-2-fast' },
					{ name: 'Composer 2', value: 'composer-2' },
					{ name: 'Composer 1.5', value: 'composer-1.5' },
					// Claude 4.6 models
					{ name: 'Claude 4.6 Opus High', value: 'claude-4.6-opus-high' },
					{ name: 'Claude 4.6 Opus High Thinking', value: 'claude-4.6-opus-high-thinking' },
					{ name: 'Claude 4.6 Opus Max', value: 'claude-4.6-opus-max' },
					{ name: 'Claude 4.6 Opus Max Thinking', value: 'claude-4.6-opus-max-thinking' },
					{ name: 'Claude 4.6 Sonnet Medium', value: 'claude-4.6-sonnet-medium' },
					{ name: 'Claude 4.6 Sonnet Medium Thinking', value: 'claude-4.6-sonnet-medium-thinking' },
					// Claude 4.5 models
					{ name: 'Claude 4.5 Opus High', value: 'claude-4.5-opus-high' },
					{ name: 'Claude 4.5 Opus High Thinking', value: 'claude-4.5-opus-high-thinking' },
					{ name: 'Claude 4.5 Sonnet', value: 'claude-4.5-sonnet' },
					{ name: 'Claude 4.5 Sonnet Thinking', value: 'claude-4.5-sonnet-thinking' },
					// Claude 4 models
					{ name: 'Claude 4 Sonnet', value: 'claude-4-sonnet' },
					{ name: 'Claude 4 Sonnet 1M', value: 'claude-4-sonnet-1m' },
					{ name: 'Claude 4 Sonnet Thinking', value: 'claude-4-sonnet-thinking' },
					{ name: 'Claude 4 Sonnet 1M Thinking', value: 'claude-4-sonnet-1m-thinking' },
					// Gemini models
					{ name: 'Gemini 3.1 Pro', value: 'gemini-3.1-pro' },
					{ name: 'Gemini 3 Flash', value: 'gemini-3-flash' },
					// GPT-5.4 models
					{ name: 'GPT-5.4 Low', value: 'gpt-5.4-low' },
					{ name: 'GPT-5.4 Medium', value: 'gpt-5.4-medium' },
					{ name: 'GPT-5.4 Medium Fast', value: 'gpt-5.4-medium-fast' },
					{ name: 'GPT-5.4 High', value: 'gpt-5.4-high' },
					{ name: 'GPT-5.4 High Fast', value: 'gpt-5.4-high-fast' },
					{ name: 'GPT-5.4 XHigh', value: 'gpt-5.4-xhigh' },
					{ name: 'GPT-5.4 XHigh Fast', value: 'gpt-5.4-xhigh-fast' },
					{ name: 'GPT-5.4 Mini None', value: 'gpt-5.4-mini-none' },
					{ name: 'GPT-5.4 Mini Low', value: 'gpt-5.4-mini-low' },
					{ name: 'GPT-5.4 Mini Medium', value: 'gpt-5.4-mini-medium' },
					{ name: 'GPT-5.4 Mini High', value: 'gpt-5.4-mini-high' },
					{ name: 'GPT-5.4 Mini XHigh', value: 'gpt-5.4-mini-xhigh' },
					{ name: 'GPT-5.4 Nano None', value: 'gpt-5.4-nano-none' },
					{ name: 'GPT-5.4 Nano Low', value: 'gpt-5.4-nano-low' },
					{ name: 'GPT-5.4 Nano Medium', value: 'gpt-5.4-nano-medium' },
					{ name: 'GPT-5.4 Nano High', value: 'gpt-5.4-nano-high' },
					{ name: 'GPT-5.4 Nano XHigh', value: 'gpt-5.4-nano-xhigh' },
					// GPT-5.3 Codex models
					{ name: 'GPT-5.3 Codex Low', value: 'gpt-5.3-codex-low' },
					{ name: 'GPT-5.3 Codex Low Fast', value: 'gpt-5.3-codex-low-fast' },
					{ name: 'GPT-5.3 Codex', value: 'gpt-5.3-codex' },
					{ name: 'GPT-5.3 Codex Fast', value: 'gpt-5.3-codex-fast' },
					{ name: 'GPT-5.3 Codex High', value: 'gpt-5.3-codex-high' },
					{ name: 'GPT-5.3 Codex High Fast', value: 'gpt-5.3-codex-high-fast' },
					{ name: 'GPT-5.3 Codex XHigh', value: 'gpt-5.3-codex-xhigh' },
					{ name: 'GPT-5.3 Codex XHigh Fast', value: 'gpt-5.3-codex-xhigh-fast' },
					{ name: 'GPT-5.3 Codex Spark Preview Low', value: 'gpt-5.3-codex-spark-preview-low' },
					{ name: 'GPT-5.3 Codex Spark Preview', value: 'gpt-5.3-codex-spark-preview' },
					{ name: 'GPT-5.3 Codex Spark Preview High', value: 'gpt-5.3-codex-spark-preview-high' },
					{ name: 'GPT-5.3 Codex Spark Preview XHigh', value: 'gpt-5.3-codex-spark-preview-xhigh' },
					// GPT-5.2 models
					{ name: 'GPT-5.2 Low', value: 'gpt-5.2-low' },
					{ name: 'GPT-5.2 Low Fast', value: 'gpt-5.2-low-fast' },
					{ name: 'GPT-5.2', value: 'gpt-5.2' },
					{ name: 'GPT-5.2 Fast', value: 'gpt-5.2-fast' },
					{ name: 'GPT-5.2 High', value: 'gpt-5.2-high' },
					{ name: 'GPT-5.2 High Fast', value: 'gpt-5.2-high-fast' },
					{ name: 'GPT-5.2 XHigh', value: 'gpt-5.2-xhigh' },
					{ name: 'GPT-5.2 XHigh Fast', value: 'gpt-5.2-xhigh-fast' },
					{ name: 'GPT-5.2 Codex Low', value: 'gpt-5.2-codex-low' },
					{ name: 'GPT-5.2 Codex Low Fast', value: 'gpt-5.2-codex-low-fast' },
					{ name: 'GPT-5.2 Codex', value: 'gpt-5.2-codex' },
					{ name: 'GPT-5.2 Codex Fast', value: 'gpt-5.2-codex-fast' },
					{ name: 'GPT-5.2 Codex High', value: 'gpt-5.2-codex-high' },
					{ name: 'GPT-5.2 Codex High Fast', value: 'gpt-5.2-codex-high-fast' },
					{ name: 'GPT-5.2 Codex XHigh', value: 'gpt-5.2-codex-xhigh' },
					{ name: 'GPT-5.2 Codex XHigh Fast', value: 'gpt-5.2-codex-xhigh-fast' },
					// GPT-5.1 models
					{ name: 'GPT-5.1 Low', value: 'gpt-5.1-low' },
					{ name: 'GPT-5.1', value: 'gpt-5.1' },
					{ name: 'GPT-5.1 High', value: 'gpt-5.1-high' },
					{ name: 'GPT-5.1 Codex Max Low', value: 'gpt-5.1-codex-max-low' },
					{ name: 'GPT-5.1 Codex Max Low Fast', value: 'gpt-5.1-codex-max-low-fast' },
					{ name: 'GPT-5.1 Codex Max Medium', value: 'gpt-5.1-codex-max-medium' },
					{ name: 'GPT-5.1 Codex Max Medium Fast', value: 'gpt-5.1-codex-max-medium-fast' },
					{ name: 'GPT-5.1 Codex Max High', value: 'gpt-5.1-codex-max-high' },
					{ name: 'GPT-5.1 Codex Max High Fast', value: 'gpt-5.1-codex-max-high-fast' },
					{ name: 'GPT-5.1 Codex Max XHigh', value: 'gpt-5.1-codex-max-xhigh' },
					{ name: 'GPT-5.1 Codex Max XHigh Fast', value: 'gpt-5.1-codex-max-xhigh-fast' },
					{ name: 'GPT-5.1 Codex Mini Low', value: 'gpt-5.1-codex-mini-low' },
					{ name: 'GPT-5.1 Codex Mini', value: 'gpt-5.1-codex-mini' },
					{ name: 'GPT-5.1 Codex Mini High', value: 'gpt-5.1-codex-mini-high' },
					// GPT-5 models
					{ name: 'GPT-5 Mini', value: 'gpt-5-mini' },
					// Grok models
					{ name: 'Grok 4 20', value: 'grok-4-20' },
					{ name: 'Grok 4 20 Thinking', value: 'grok-4-20-thinking' },
					// Kimi models
					{ name: 'Kimi K2.5', value: 'kimi-k2.5' },
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
						default: 'cursor-agent',
						description:
							'Path to the cursor-agent binary. Defaults to "cursor-agent" (must be in PATH).',
						type: 'string',
					},
					{
						displayName: 'Working Directory',
						name: 'workingDirectory',
						default: '',
						description:
							'Working directory for the cursor-agent process. Leave empty to use the default.',
						type: 'string',
					},
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const modelName = this.getNodeParameter('model', itemIndex) as string;

		const binaryPath = this.getNodeParameter(
			'options.binaryPath',
			itemIndex,
			'cursor-agent',
		) as string;
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

		console.log('[LmChatCursorAgent] resolved Cursor Agent options', {
			itemIndex,
			modelName,
			binaryPath,
			rawWorkingDirectory,
			workingDirectory: normalizedWorkingDirectory,
		});

		if (isWorkingDirectoryExpression && !normalizedWorkingDirectory) {
			throw new ApplicationError(
				`Cursor Agent working directory expression resolved to an empty value: ${rawWorkingDirectoryValue}`,
			);
		}

		if (
			normalizedWorkingDirectory.includes('{{') ||
			normalizedWorkingDirectory.includes('$workspace')
		) {
			throw new ApplicationError(
				`Cursor Agent working directory was not resolved before execution: ${normalizedWorkingDirectory}`,
			);
		}

		if (
			normalizedWorkingDirectory &&
			(!existsSync(normalizedWorkingDirectory) ||
				!statSync(normalizedWorkingDirectory).isDirectory())
		) {
			throw new ApplicationError(
				`Cursor Agent working directory does not exist or is not a directory: ${normalizedWorkingDirectory}`,
			);
		}

		const model = new ChatCursorAgentCLI({
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
