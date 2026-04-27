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

interface CodexCliFields {
	model: string;
	binaryPath: string;
	workingDirectory: string;
	sandboxMode: string;
}

interface ParsedToolCall {
	id: string;
	name: string;
	args: Record<string, unknown>;
}

interface ParsedJsonlResult {
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
 * Custom LangChain chat model that wraps the OpenAI Codex CLI binary.
 * Uses `codex exec --json` for non-interactive execution.
 * Supports tool calling by injecting tool schemas into the prompt
 * and parsing structured JSON responses for tool calls.
 */
class ChatCodexCLI extends BaseChatModel {
	model: string;

	binaryPath: string;

	workingDirectory: string;

	sandboxMode: string;

	boundTools: BindToolsInput[] = [];

	constructor(fields: CodexCliFields) {
		super({});
		this.model = fields.model;
		this.binaryPath = fields.binaryPath;
		this.workingDirectory = fields.workingDirectory;
		this.sandboxMode = fields.sandboxMode;
	}

	_llmType(): string {
		return 'codex-cli';
	}

	override bindTools(tools: BindToolsInput[], kwargs?: Partial<this['ParsedCallOptions']>) {
		const clone = new ChatCodexCLI({
			model: this.model,
			binaryPath: this.binaryPath,
			workingDirectory: this.workingDirectory,
			sandboxMode: this.sandboxMode,
		});
		clone.boundTools = tools;
		clone.callbacks = this.callbacks;
		if (kwargs) {
			return (
				clone as unknown as {
					bind: (kwargs: Record<string, unknown>) => ChatCodexCLI;
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
		console.log('[LmChatCodexCli] _generate called', {
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

		console.log('[LmChatCodexCli] prompt built, length:', prompt.length);

		// Execute codex CLI
		const rawResponse = await this.executeCodexCli(prompt);

		console.log('[LmChatCodexCli] raw response received, length:', rawResponse.length);

		// Check for tool calls in response
		if (this.boundTools.length > 0) {
			const toolCalls = this.extractToolCalls(rawResponse);
			if (toolCalls.length > 0) {
				console.log('[LmChatCodexCli] extracted tool calls:', toolCalls.length);
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
		console.log('[LmChatCodexCli] returning text response');
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

	private async executeCodexCli(prompt: string): Promise<string> {
		// Build args: codex exec --json --skip-git-repo-check --full-auto [--sandbox <mode>] [--model <model>] [--cd <dir>] -
		const args = ['exec', '--json', '--skip-git-repo-check', '--full-auto'];

		if (this.sandboxMode) {
			args.push('--sandbox', this.sandboxMode);
		}

		if (this.model && this.model !== 'auto') {
			args.push('--model', this.model);
		}

		const cwd = this.workingDirectory?.trim() || undefined;
		if (cwd) {
			args.push('--cd', cwd);
		}

		// Use `-` to read prompt from stdin
		args.push('-');

		console.log('[LmChatCodexCli] spawning codex exec', {
			binaryPath: this.binaryPath,
			args,
			model: this.model,
			cwd,
			sandboxMode: this.sandboxMode,
		});

		return await new Promise<string>((resolve, reject) => {
			const child = spawn(this.binaryPath, args, {
				// Codex uses --cd for working directory, so we don't set cwd on spawn
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
				console.error('[LmChatCodexCli] spawn error:', err.message);
				reject(
					new Error(
						`Failed to spawn codex: ${err.message}. Make sure the Codex CLI is installed (npm install -g @openai/codex) and accessible.`,
					),
				);
			});

			child.on('close', (code: number | null) => {
				console.log('[LmChatCodexCli] codex exec exited', {
					code,
					stdoutLength: stdout.length,
					stderrLength: stderr.length,
				});

				// Parse the JSONL output — even on non-zero exit code, stdout may contain
				// useful JSONL events (e.g. error messages from the Codex API)
				const parseResult = this.parseJsonlOutput(stdout);

				if (parseResult.assistantText) {
					console.log(
						'[LmChatCodexCli] parsed assistant content, length:',
						parseResult.assistantText.length,
					);
					resolve(parseResult.assistantText);
					return;
				}

				// No assistant response — build a meaningful error from available info
				if (parseResult.errorMessage) {
					console.error('[LmChatCodexCli] codex returned error:', parseResult.errorMessage);
					reject(new Error(`Codex CLI error: ${parseResult.errorMessage}`));
					return;
				}

				if (code !== 0) {
					const stderrMsg = stderr.trim();
					const errorMsg = stderrMsg || `codex exec exited with code ${code}`;
					console.error('[LmChatCodexCli] codex exec failed with code', code, ':', errorMsg);
					reject(new Error(errorMsg));
					return;
				}

				console.error(
					'[LmChatCodexCli] no assistant response parsed from output, stdout preview:',
					stdout.substring(0, 500),
				);
				reject(new Error('No assistant response received from codex exec'));
			});

			if (child.stdin) {
				child.stdin.write(prompt);
				child.stdin.end();
			}
		});
	}

	/**
	 * Parse JSONL output from `codex exec --json`.
	 *
	 * Actual event types from codex exec --json (verified empirically):
	 * - {"type":"thread.started","thread_id":"..."}
	 * - {"type":"turn.started"}
	 * - {"type":"item.started","item":{"type":"agent_message",...}}
	 * - {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
	 * - {"type":"turn.completed"}
	 * - {"type":"error","message":"..."}
	 * - {"type":"turn.failed","error":{"message":"..."}}
	 *
	 * Returns both assistant text and any error messages found.
	 */
	private parseJsonlOutput(output: string): ParsedJsonlResult {
		const lines = output.split('\n').filter((line) => line.trim());
		const assistantParts: string[] = [];
		const errorParts: string[] = [];

		console.log('[LmChatCodexCli] parsing JSONL output, line count:', lines.length);

		for (const line of lines) {
			try {
				const parsed = JSON.parse(line) as Record<string, unknown>;
				const eventType = parsed.type as string | undefined;

				console.log(
					'[LmChatCodexCli] JSONL event:',
					eventType,
					'| keys:',
					Object.keys(parsed).join(','),
				);

				// item.completed with agent_message => assistant text
				if (eventType === 'item.completed') {
					const item = parsed.item as Record<string, unknown> | undefined;
					if (item?.type === 'agent_message' && typeof item.text === 'string') {
						console.log(
							'[LmChatCodexCli] found agent_message text, length:',
							(item.text as string).length,
						);
						assistantParts.push(item.text as string);
					}
				}

				// Direct message events (fallback for other versions)
				if (eventType === 'message' && parsed.role === 'assistant') {
					const content = parsed.content;
					if (Array.isArray(content)) {
						for (const c of content as Array<Record<string, unknown>>) {
							if (c.type === 'text' && typeof c.text === 'string') {
								assistantParts.push(c.text as string);
							}
						}
					} else if (typeof content === 'string') {
						assistantParts.push(content);
					}
				}

				// Nested assistant message (cursor-agent compatibility)
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
				if (eventType === 'error' && typeof parsed.message === 'string') {
					errorParts.push(parsed.message);
				}

				// turn.failed with error
				if (eventType === 'turn.failed') {
					const error = parsed.error as Record<string, unknown> | undefined;
					if (error && typeof error.message === 'string') {
						errorParts.push(error.message);
					}
				}
			} catch {
				// Skip non-JSON lines (e.g. progress output)
			}
		}

		return {
			assistantText: assistantParts.join(''),
			errorMessage: errorParts.join('; '),
		};
	}
}

export class LmChatCodexCli implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Codex CLI Chat Model',

		name: 'lmChatCodexCli',
		icon: 'file:codexCli.svg',
		group: ['transform'],
		version: [1],
		description:
			'Chat model powered by the OpenAI Codex CLI. Requires codex to be installed locally (npm install -g @openai/codex).',
		defaults: {
			name: 'Codex CLI Chat Model',
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
				description: 'The model to use via codex CLI',
				// eslint-disable-next-line n8n-nodes-base/node-param-options-type-unsorted-items
				options: [
					{ name: 'Auto (Default)', value: 'auto' },
					// GPT-5 series (supported by Codex CLI)
					{ name: 'GPT-5.5', value: 'gpt-5.5' },
					{ name: 'GPT-5.5 Fast', value: 'gpt-5.5-fast' },
					{ name: 'GPT-5.4', value: 'gpt-5.4' },
					{ name: 'GPT-5.4 Fast', value: 'gpt-5.4-fast' },
					{ name: 'GPT-5.4 Mini', value: 'gpt-5.4-mini' },
					{ name: 'GPT-5.3 Codex', value: 'gpt-5.3-codex' },
					{ name: 'GPT-5.3 Codex Spark', value: 'gpt-5.3-codex-spark' },
					{ name: 'GPT-5.2', value: 'gpt-5.2' },
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
						default: 'codex',
						description: 'Path to the codex binary. Defaults to "codex" (must be in PATH).',
						type: 'string',
					},
					{
						displayName: 'Working Directory',
						name: 'workingDirectory',
						default: '',
						description: 'Working directory for the codex process. Leave empty to use the default.',
						type: 'string',
					},
					{
						displayName: 'Sandbox Mode',
						name: 'sandboxMode',
						type: 'options',
						default: 'read-only',
						description: 'Sandbox policy for executing model-generated shell commands',
						options: [
							{
								name: 'Read Only',
								value: 'read-only',
								description: 'Only allow read operations (safest)',
							},
							{
								name: 'Workspace Write',
								value: 'workspace-write',
								description: 'Allow writes within the workspace directory',
							},
							{
								name: 'Full Access (Dangerous)',
								value: 'danger-full-access',
								description: 'Full filesystem access — use with extreme caution',
							},
						],
					},
				],
			},
		],
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const modelName = this.getNodeParameter('model', itemIndex) as string;

		const binaryPath = this.getNodeParameter('options.binaryPath', itemIndex, 'codex') as string;
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

		const sandboxMode = this.getNodeParameter(
			'options.sandboxMode',
			itemIndex,
			'read-only',
		) as string;

		console.log('[LmChatCodexCli] resolved Codex CLI options', {
			itemIndex,
			modelName,
			binaryPath,
			rawWorkingDirectory,
			workingDirectory: normalizedWorkingDirectory,
			sandboxMode,
		});

		if (isWorkingDirectoryExpression && !normalizedWorkingDirectory) {
			throw new ApplicationError(
				`Codex CLI working directory expression resolved to an empty value: ${rawWorkingDirectoryValue}`,
			);
		}

		if (
			normalizedWorkingDirectory.includes('{{') ||
			normalizedWorkingDirectory.includes('$workspace')
		) {
			throw new ApplicationError(
				`Codex CLI working directory was not resolved before execution: ${normalizedWorkingDirectory}`,
			);
		}

		if (
			normalizedWorkingDirectory &&
			(!existsSync(normalizedWorkingDirectory) ||
				!statSync(normalizedWorkingDirectory).isDirectory())
		) {
			throw new ApplicationError(
				`Codex CLI working directory does not exist or is not a directory: ${normalizedWorkingDirectory}`,
			);
		}

		console.log('[LmChatCodexCli] creating ChatCodexCLI instance', {
			model: modelName,
			binaryPath,
			workingDirectory: normalizedWorkingDirectory,
			sandboxMode,
		});

		const model = new ChatCodexCLI({
			model: modelName,
			binaryPath,
			workingDirectory: normalizedWorkingDirectory,
			sandboxMode,
		});

		model.callbacks = [new N8nLlmTracing(this)];

		return {
			response: model,
		};
	}
}
