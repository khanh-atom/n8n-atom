/**
 * Run Workflow — Execute an n8n workflow file via the n8n REST API.
 *
 * Lightweight equivalent of packages/cli/src/commands/run.ts.
 * Zero n8n dependencies — only uses built-in Node.js APIs (fs, path, fetch).
 */
import fs from 'fs';
import path from 'path';

const LOG_PREFIX = '[n8n-run]';

function log(msg) {
	console.error(`${LOG_PREFIX} ${msg}`);
}

/**
 * If value is a string that parses as JSON, return the parsed value; otherwise return as-is.
 * Used so that execution data's "data" property (often a minified JSON string) is written multi-line.
 */
function tryParseJson(value) {
	if (typeof value !== 'string' || !value.trim()) {
		return value;
	}
	const first = value.trim().charAt(0);
	if (first !== '[' && first !== '{') {
		return value;
	}
	try {
		return JSON.parse(value);
	} catch {
		return value;
	}
}

/**
 * Recursively expand any property named "data" that is a JSON string into an object/array
 * so that the final JSON file is pretty-printed with multiple lines.
 * Replicates formatExecutionDataForFile from n8n-atom-vscodev3.
 */
function formatExecutionData(obj) {
	if (obj === null || typeof obj !== 'object') {
		return obj;
	}
	if (Array.isArray(obj)) {
		return obj.map((item) => formatExecutionData(item));
	}
	const out = {};
	for (const [key, val] of Object.entries(obj)) {
		if (key === 'data' && typeof val === 'string') {
			const parsed = tryParseJson(val);
			out[key] =
				typeof parsed === 'object' && parsed !== null
					? formatExecutionData(parsed)
					: parsed;
		} else {
			out[key] = formatExecutionData(val);
		}
	}
	return out;
}

/**
 * Extract and format execution data for .data file.
 * When execution has resultData.runData (per-node run data), use that.
 * Otherwise fall back to the full execution data object.
 * Replicates getExecutionDataFileContent from n8n-atom-vscodev3.
 */
function getExecutionDataFileContent(executionData) {
	const runData = executionData?.resultData?.runData;
	if (runData && typeof runData === 'object' && !Array.isArray(runData)) {
		return formatExecutionData(runData);
	}
	return formatExecutionData(executionData);
}

/**
 * Run an n8n workflow file.
 *
 * @param {string} filePath - Path to the .n8n workflow file
 * @param {{ input?: string, port?: number, raw?: boolean }} [options]
 */
export async function runWorkflow(filePath, options = {}) {
	const { input, raw = false } = options;
	const port = options.port ?? parseInt(process.env.N8N_PORT ?? '5888', 10);
	const serverUrl = `http://localhost:${port}`;

	// ── Step 1: Read and parse the .n8n file ──────────────────────
	const resolvedPath = path.resolve(filePath);
	log(`── READING FILE ──`);
	log(`File path: ${resolvedPath}`);

	if (!fs.existsSync(resolvedPath)) {
		throw new Error(`The workflow file does not exist: ${resolvedPath}`);
	}

	const fileStat = fs.statSync(resolvedPath);
	log(`File size: ${fileStat.size} bytes, Last modified: ${fileStat.mtime.toISOString()}`);

	let fileData;
	try {
		const fileContent = fs.readFileSync(resolvedPath, { encoding: 'utf8' });
		fileData = JSON.parse(fileContent);
		log(`Successfully parsed workflow file.`);
		log(`  Name: "${fileData.name}"`);
		log(`  ID: "${fileData.id ?? 'none'}"`);
		log(`  Nodes (${fileData.nodes?.length ?? 0}):`);
		if (fileData.nodes && Array.isArray(fileData.nodes)) {
			for (const node of fileData.nodes) {
				log(`    - "${node.name}" (type: ${node.type}, version: ${node.typeVersion})`);
			}
		}
	} catch (error) {
		throw new Error(
			`Failed to parse workflow file: ${error instanceof Error ? error.message : String(error)}`,
		);
	}

	// ── Step 2: Health check ─────────────────────────────────────
	log(`── EXECUTING ──`);
	log(`n8n server URL: ${serverUrl}`);

	try {
		const healthResponse = await fetch(`${serverUrl}/rest/cli/health`);
		if (!healthResponse.ok) {
			throw new Error(`Health check returned ${healthResponse.status}`);
		}
		log(`Server is reachable.`);
	} catch (error) {
		throw new Error(
			`Cannot reach n8n server at ${serverUrl}. Is the server running? Error: ${error instanceof Error ? error.message : String(error)}`,
		);
	}

	// ── Step 3: POST to the synchronous CLI API ──────────────────
	const executeUrl = `${serverUrl}/rest/cli/run`;
	log(`POST ${executeUrl}`);

	const requestBody = {
		workflowData: fileData,
		fileModifiedAt: fileStat.mtime.toISOString(),
	};

	// Pass input based on content: JSON objects → inputData, strings → chatInput
	if (input !== undefined) {
		try {
			const parsed = JSON.parse(input);
			if (typeof parsed === 'object' && parsed !== null) {
				requestBody.inputData = parsed;
			} else {
				requestBody.chatInput = String(input);
			}
		} catch {
			requestBody.chatInput = input;
		}
	} else {
		// No input provided — check for executeWorkflowTrigger with default values
		const triggerNode = fileData.nodes?.find(
			(n) => n.type === 'n8n-nodes-base.executeWorkflowTrigger',
		);
		if (triggerNode) {
			const workflowInputs = triggerNode.parameters?.workflowInputs?.values ?? [];
			const defaults = {};
			for (const field of workflowInputs) {
				if (field.name && field.defaultValue !== undefined && field.defaultValue !== '') {
					defaults[field.name] = field.defaultValue;
				}
			}
			if (Object.keys(defaults).length > 0) {
				log(`Using default values: ${JSON.stringify(defaults)}`);
				requestBody.inputData = defaults;
			}
		}
	}

	// Auto-inject workflow filepath into inputData
	if (!requestBody.inputData) {
		requestBody.inputData = {};
	}
	requestBody.inputData.__filepath = resolvedPath;
	requestBody.inputData.__dirpath = path.dirname(resolvedPath);

	const response = await fetch(executeUrl, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(requestBody),
	});

	log(`Response status: ${response.status} ${response.statusText}`);

	if (!response.ok) {
		const errorBody = await response.text();
		log(`API error response: ${errorBody}`);
		throw new Error(
			`Failed to execute workflow: ${response.status} ${response.statusText} — ${errorBody}`,
		);
	}

	// ── Step 4: Display results ──────────────────────────────────
	const result = await response.json();

	log(`── RESULTS ──`);
	log(
		`Execution ID: ${result.executionId ?? 'unknown'}, Status: ${result.status ?? 'unknown'}, Time: ${result.executionTime ?? '?'}s`,
	);

	if (result.success) {
		if (!raw) {
			log('✅ Execution was successful!');
			log('====================================');
		}
	} else {
		log('❌ Execution FAILED');
		log('====================================');
		if (result.error) {
			log(`Error: ${result.error}`);
		}
	}

	// Log per-node results
	if (result.data?.runData && !raw) {
		const runData = result.data.runData;
		const nodeNames = Object.keys(runData);
		log(`Nodes executed (${nodeNames.length}): ${nodeNames.join(' → ')}`);

		for (const [nodeName, nodeRuns] of Object.entries(runData)) {
			for (const nodeRun of nodeRuns) {
				const status = nodeRun.executionStatus ?? 'unknown';
				const time = nodeRun.executionTime ?? 0;
				log(`  ✅ "${nodeName}" — status: ${status}, time: ${time}ms`);

				if (nodeRun.data?.main) {
					for (const outputBranch of nodeRun.data.main) {
						if (outputBranch) {
							log(`    Output items: ${outputBranch.length}`);
							for (const item of outputBranch) {
								if (item.json) {
									log(`    → ${JSON.stringify(item.json)}`);
								}
							}
						}
					}
				}
			}
		}
	}

	// ── Step 5: Generate .data file ──────────────────────────────
	if (result.data) {
		try {
			const dataFilePath = resolvedPath.replace(/\.n8n$/, '.data');
			const formatted = getExecutionDataFileContent(result.data);
			const dataContent = JSON.stringify(formatted, null, 2);
			fs.writeFileSync(dataFilePath, dataContent, { encoding: 'utf8' });
			log(`Data file saved to: ${dataFilePath}`);
		} catch (dataError) {
			log(`Warning: Failed to save .data file: ${dataError instanceof Error ? dataError.message : String(dataError)}`);
		}
	}

	// ── Step 6: Sync workflow back to file if server had newer version ──
	if (result.syncedWorkflow) {
		log(`Server had a newer workflow version — updating file: ${resolvedPath}`);
		const syncedData = result.syncedWorkflow;
		// Strip runtime-injected __filepath and __dirpath from pinData
		if (syncedData.pinData && typeof syncedData.pinData === 'object') {
			for (const items of Object.values(syncedData.pinData)) {
				if (Array.isArray(items)) {
					for (const item of items) {
						if (item.json) {
							delete item.json.__filepath;
							delete item.json.__dirpath;
						}
					}
				}
			}
		}
		const fileContent = JSON.stringify(syncedData, null, 2) + '\n';
		fs.writeFileSync(resolvedPath, fileContent, { encoding: 'utf8' });
		log(`File updated with server workflow.`);
	}

	// Output the full result as JSON to stdout
	// console.log(JSON.stringify(result, null, 2));
	log(`Done.`);

	return result;
}
