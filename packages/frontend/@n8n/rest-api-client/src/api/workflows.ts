import type { IWorkflowSettings, IConnections, INode, IPinData, IDataObject } from 'n8n-workflow';

import type { ITag } from './tags';

export interface WorkflowMetadata {
	onboardingId?: string;
	templateId?: string;
	instanceId?: string;
	templateCredsSetupCompleted?: boolean;
}

// Simple version of n8n-workflow.Workflow
export interface WorkflowData {
	id?: string;
	name?: string;
	active?: boolean;
	nodes: INode[];
	connections: IConnections;
	settings?: IWorkflowSettings;
	tags?: string[];
	pinData?: IPinData;
	versionId?: string;
	activeVersionId?: string | null;
	meta?: WorkflowMetadata;
	/**
	 * Workspace context (e.g. `__filePath` / `__dirPath` from the
	 * VS Code extension webview). Forwarded to the backend on manual run so
	 * `$workspace` resolves at execution time. Persisted to DB but never
	 * written to the `.n8n` file on disk.
	 */
	workspace?: IDataObject;
}

export interface WorkflowDataUpdate {
	id?: string;
	name?: string;
	description?: string | null;
	nodes?: INode[];
	connections?: IConnections;
	settings?: IWorkflowSettings;
	active?: boolean;
	tags?: ITag[] | string[]; // string[] when store or requested, ITag[] from API response
	pinData?: IPinData;
	versionId?: string;
	meta?: WorkflowMetadata;
	parentFolderId?: string;
	uiContext?: string;
	// checksum of workflow snapshot for conflict detection
	expectedChecksum?: string;
	aiBuilderAssisted?: boolean;
	autosaved?: boolean;
	/** Workspace context persisted to DB for $workspace access in browser. */
	workspace?: IDataObject;
}

export interface WorkflowDataCreate extends WorkflowDataUpdate {
	projectId?: string;
}
