-- Seed database_backup action type (Trove instance backup)
INSERT IGNORE INTO action_types (action_type, name, description, action_parameters_schema, enabled)
VALUES (
  'database_backup',
  'Database backup',
  'Create a Trove backup of an existing database instance',
  JSON_OBJECT(
    'type', 'object',
    'properties', JSON_OBJECT(
      'database_instance_id', JSON_OBJECT('type', 'string'),
      'backup_name_prefix', JSON_OBJECT('type', 'string'),
      'description', JSON_OBJECT('type', 'string')
    ),
    'required', JSON_ARRAY('database_instance_id'),
    'additionalProperties', true
  ),
  TRUE
);
