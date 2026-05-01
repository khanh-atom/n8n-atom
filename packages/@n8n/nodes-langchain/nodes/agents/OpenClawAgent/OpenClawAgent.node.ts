import type { INodeTypeBaseDescription, IVersionedNodeType } from 'n8n-workflow';
import { VersionedNodeType } from 'n8n-workflow';

import { OpenClawAgentV1 } from './V1/OpenClawAgentV1.node';
import { OpenClawAgentV2 } from './V2/OpenClawAgentV2.node';

export class OpenClawAgent extends VersionedNodeType {
	constructor() {
		const baseDescription: INodeTypeBaseDescription = {
			displayName: 'OpenClaw AI Agent',
			name: 'openClawAgent',
			icon: 'file:openclaw.svg',
			group: ['trigger', 'transform'],
			description: 'Runs a one-shot OpenClaw agent turn through the OpenClaw CLI',
			codex: {
				alias: ['OpenClaw', 'Agent', 'Gateway', 'Assistant', 'Channel'],
				categories: ['AI'],
				subcategories: {
					AI: ['Agents', 'Root Nodes'],
				},
				resources: {
					primaryDocumentation: [
						{
							url: 'https://docs.openclaw.ai/cli/agent',
						},
					],
				},
			},
			defaultVersion: 2,
		};

		const nodeVersions: IVersionedNodeType['nodeVersions'] = {
			1: new OpenClawAgentV1(baseDescription),
			2: new OpenClawAgentV2(baseDescription),
		};

		super(nodeVersions, baseDescription);
	}
}
