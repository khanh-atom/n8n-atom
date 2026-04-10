import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class NineRouterApi implements ICredentialType {
	name = 'nineRouterApi';

	displayName = '9Router';

	documentationUrl = 'nineRouter';

	properties: INodeProperties[] = [
		{
			displayName: 'API Key',
			name: 'apiKey',
			type: 'string',
			typeOptions: { password: true },
			required: false,
			default: '',
			description: 'Optional API key if REQUIRE_API_KEY is enabled on your 9Router instance',
		},
		{
			displayName: 'Base URL',
			name: 'url',
			type: 'string',
			default: 'http://localhost:20128/api/v1',
			description: 'Base URL of your 9Router instance',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Authorization: '=Bearer {{$credentials.apiKey}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: '={{ $credentials.url }}',
			url: '/models',
		},
	};
}
