import type {
	ITriggerFunctions,
	IDataObject,
	INodeType,
	INodeTypeDescription,
	ITriggerResponse,
} from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';

import { apiRequest } from './GenericFunctions';

/**
 * Telegram Polling Trigger — uses getUpdates (true long-polling) instead of webhooks.
 *
 * This lets the trigger work on local / HTTP-only n8n instances without
 * requiring an HTTPS URL or a tunnelling service like ngrok.
 *
 * Design mirrors openclaw's approach: continuous loop calling getUpdates with
 * a 30-second long-poll timeout, filtering by allowed update types, and
 * persisting the offset across restarts.
 */
export class TelegramPollingTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Telegram Polling Trigger',
		name: 'telegramPollingTrigger',
		icon: 'file:telegram.svg',
		group: ['trigger'],
		version: 1,
		subtitle: '=Updates: {{$parameter["updates"].join(", ")}}',
		description:
			'Starts the workflow on a Telegram update using long-polling (no HTTPS required, works locally)',
		defaults: {
			name: 'Telegram Polling Trigger',
		},
		inputs: [],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'telegramApi',
				required: true,
			},
		],
		triggerPanel: {
			header: '',
			executionsHelp: {
				inactive:
					"<b>While building your workflow</b>, click the 'execute step' button, then send a message to your Telegram bot. This will trigger an execution, which will show up in this editor.<br /><br /><b>Once you're happy with your workflow</b>, publish it. Then every time a message arrives, the workflow will execute. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
				active:
					"<b>While building your workflow</b>, click the 'execute step' button, then send a message to your Telegram bot. This will trigger an execution, which will show up in this editor.<br /><br /><b>Your workflow will also execute automatically</b>, since it's activated. Every time a message arrives, this node will trigger an execution. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
			},
			activationHint:
				"Once you've finished building your workflow, publish it to have it also listen continuously (you just won't see those executions here).",
		},
		properties: [
			{
				displayName:
					'This trigger uses long-polling (getUpdates) instead of webhooks, so it works locally without HTTPS. Any existing webhook for this bot will be removed when the trigger starts.',
				name: 'pollingNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName:
					'Due to Telegram API limitations, you can use just one Telegram trigger for each bot at a time',
				name: 'telegramTriggerNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Trigger On',
				name: 'updates',
				type: 'multiOptions',
				options: [
					{
						name: '*',
						value: '*',
						description: 'All updates',
					},
					{
						name: 'Callback Query',
						value: 'callback_query',
						description: 'Trigger on new incoming callback query',
					},
					{
						name: 'Channel Post',
						value: 'channel_post',
						description:
							'Trigger on new incoming channel post of any kind — text, photo, sticker, etc',
					},
					{
						name: 'Edited Channel Post',
						value: 'edited_channel_post',
						description:
							'Trigger on new version of a channel post that is known to the bot and was edited',
					},
					{
						name: 'Edited Message',
						value: 'edited_message',
						description:
							'Trigger on new version of a message that is known to the bot and was edited',
					},
					{
						name: 'Inline Query',
						value: 'inline_query',
						description: 'Trigger on new incoming inline query',
					},
					{
						name: 'Message',
						value: 'message',
						description: 'Trigger on new incoming message of any kind — text, photo, sticker, etc',
					},
					{
						name: 'Poll',
						value: 'poll',
						action: 'On Poll Change',
						description:
							'Trigger on new poll state. Bots receive only updates about stopped polls and polls, which are sent by the bot.',
					},
					{
						name: 'Pre-Checkout Query',
						value: 'pre_checkout_query',
						description:
							'Trigger on new incoming pre-checkout query. Contains full information about checkout.',
					},
					{
						name: 'Shipping Query',
						value: 'shipping_query',
						description:
							'Trigger on new incoming shipping query. Only for invoices with flexible price.',
					},
				],
				required: true,
				default: [],
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				options: [
					{
						displayName: 'Restrict to Chat IDs',
						name: 'chatIds',
						type: 'string',
						default: '',
						description:
							'The chat IDs to restrict the trigger to. Multiple can be defined separated by comma.',
					},
					{
						displayName: 'Restrict to User IDs',
						name: 'userIds',
						type: 'string',
						default: '',
						description:
							'The user IDs to restrict the trigger to. Multiple can be defined separated by comma.',
					},
				],
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const allowedUpdates = this.getNodeParameter('updates') as string[];
		const additionalFields = this.getNodeParameter('additionalFields') as IDataObject;
		const nodeStaticData = this.getWorkflowStaticData('node');

		let aborted = false;

		// Clear any existing webhook so getUpdates works.
		// Telegram rejects getUpdates when a webhook is active.
		try {
			await apiRequest.call(this, 'POST', 'deleteWebhook', {});
		} catch {
			// Ignore — webhook may not have been set
		}

		// Build allowed_updates array for getUpdates
		const allowedUpdateTypes: string[] | undefined =
			allowedUpdates.length > 0 && !allowedUpdates.includes('*') ? allowedUpdates : undefined;

		// Backoff state for error recovery (mirrors openclaw's TELEGRAM_POLL_RESTART_POLICY)
		const BACKOFF = { initialMs: 2000, maxMs: 30_000, factor: 1.8, jitter: 0.25 };
		let consecutiveErrors = 0;

		const pollLoop = async () => {
			while (!aborted) {
				try {
					// Build getUpdates request — true long-polling with 30s timeout (like openclaw)
					const body: IDataObject = {
						timeout: 30,
					};

					// Resume from last known update_id
					const lastUpdateId = nodeStaticData.lastUpdateId as number | undefined;
					if (lastUpdateId !== undefined) {
						body.offset = lastUpdateId + 1;
					}

					if (allowedUpdateTypes) {
						body.allowed_updates = JSON.stringify(allowedUpdateTypes);
					}

					const responseData = await apiRequest.call(this, 'POST', 'getUpdates', body);
					consecutiveErrors = 0; // Reset on success

					const updates = responseData.result as IDataObject[];
					if (!Array.isArray(updates) || updates.length === 0) {
						continue;
					}

					// Track the highest update_id
					let maxUpdateId = (nodeStaticData.lastUpdateId as number) ?? 0;

					for (const update of updates) {
						const updateId = update.update_id as number;
						if (updateId > maxUpdateId) {
							maxUpdateId = updateId;
						}

						// Filter by update type
						if (allowedUpdateTypes && allowedUpdateTypes.length > 0) {
							const updateType = Object.keys(update).find((key) => key !== 'update_id');
							if (updateType && !allowedUpdateTypes.includes(updateType)) {
								continue;
							}
						}

						// Filter by chat ID
						if (additionalFields.chatIds) {
							const chatIds = (additionalFields.chatIds as string)
								.split(',')
								.map((id) => id.trim());
							const messageChatId = String(
								(update.message as IDataObject)?.chat
									? ((update.message as IDataObject).chat as IDataObject).id
									: '',
							);
							if (messageChatId && !chatIds.includes(messageChatId)) {
								continue;
							}
						}

						// Filter by user ID
						if (additionalFields.userIds) {
							const userIds = (additionalFields.userIds as string)
								.split(',')
								.map((id) => id.trim());
							const messageUserId = String(
								(update.message as IDataObject)?.from
									? ((update.message as IDataObject).from as IDataObject).id
									: '',
							);
							if (messageUserId && !userIds.includes(messageUserId)) {
								continue;
							}
						}

						// Emit each matched update as a separate workflow execution
						this.emit([this.helpers.returnJsonArray([update])]);
					}

					// Persist offset even if no updates matched filters — we consumed them
					nodeStaticData.lastUpdateId = maxUpdateId;
				} catch (error) {
					if (aborted) {
						break;
					}

					// On 409 conflict, another instance is polling — back off
					const errorCode =
						(error as { statusCode?: number }).statusCode ??
						(error as { error_code?: number }).error_code;

					if (errorCode === 409) {
						// getUpdates conflict — another bot instance is polling
						consecutiveErrors++;
					} else {
						consecutiveErrors++;
					}

					// Exponential backoff with jitter (same algorithm as openclaw)
					const baseDelay = Math.min(
						BACKOFF.maxMs,
						BACKOFF.initialMs * Math.pow(BACKOFF.factor, consecutiveErrors - 1),
					);
					const jitter = 1 + (Math.random() * 2 - 1) * BACKOFF.jitter;
					const delayMs = Math.min(BACKOFF.maxMs, Math.round(baseDelay * jitter));

					await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
				}
			}
		};

		// Manual trigger mode: wait for the first message then return
		const manualTriggerFunction = async () => {
			try {
				await apiRequest.call(this, 'POST', 'deleteWebhook', {});
			} catch {
				// Ignore
			}

			const body: IDataObject = {
				timeout: 30,
			};
			const lastUpdateId = nodeStaticData.lastUpdateId as number | undefined;
			if (lastUpdateId !== undefined) {
				body.offset = lastUpdateId + 1;
			}
			if (allowedUpdateTypes) {
				body.allowed_updates = JSON.stringify(allowedUpdateTypes);
			}

			// Keep polling until we get at least one update
			while (!aborted) {
				const responseData = await apiRequest.call(this, 'POST', 'getUpdates', body);
				const updates = responseData.result as IDataObject[];
				if (Array.isArray(updates) && updates.length > 0) {
					let maxUpdateId = (nodeStaticData.lastUpdateId as number) ?? 0;
					for (const update of updates) {
						const updateId = update.update_id as number;
						if (updateId > maxUpdateId) {
							maxUpdateId = updateId;
						}
					}
					nodeStaticData.lastUpdateId = maxUpdateId;
					this.emit([this.helpers.returnJsonArray(updates)]);
					return;
				}
				// No updates yet, loop again with long-poll
			}
		};

		// In production trigger mode, start the continuous loop
		if (this.getMode() === 'trigger') {
			void pollLoop();
		}

		async function closeFunction() {
			aborted = true;
		}

		return {
			closeFunction,
			manualTriggerFunction,
		};
	}
}
