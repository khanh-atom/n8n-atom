import type { MigrationContext, ReversibleMigration } from '../migration-types';

export class AddWorkspaceToWorkflowEntity1766200000000 implements ReversibleMigration {
	async up({ schemaBuilder: { addColumns, column } }: MigrationContext) {
		await addColumns('workflow_entity', [column('workspace').json]);
	}

	async down({ schemaBuilder: { dropColumns } }: MigrationContext) {
		await dropColumns('workflow_entity', ['workspace']);
	}
}
