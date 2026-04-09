INSERT IGNORE INTO action_types (action_type, name, description, action_parameters_schema, enabled)
VALUES (
  'server_snapshot',
  'Server snapshot',
  'Create a Glance image snapshot of a Nova server instance',
  JSON_OBJECT(
    'type','object',
    'properties', JSON_OBJECT('server_id', JSON_OBJECT('type','string')),
    'required', JSON_ARRAY('server_id'),
    'additionalProperties', true
  ),
  TRUE
);

INSERT IGNORE INTO schema_migrations (version) VALUES ('0002_seed_action_types');
