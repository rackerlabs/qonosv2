-- Seed swift_object_versioning action type (snapshot mode only)
INSERT IGNORE INTO action_types (action_type, name, description, action_parameters_schema, enabled)
VALUES (
  'swift_object_versioning',
  'Swift object versioning',
  'Version Swift container objects (snapshot mode copies into an archive container)',
  JSON_OBJECT(
    'type', 'object',
    'properties', JSON_OBJECT(
      'container', JSON_OBJECT('type', 'string'),
      'target_container', JSON_OBJECT('type', 'string'),
      'object_prefix', JSON_OBJECT('type', 'string'),
      'mode', JSON_OBJECT('type', 'string', 'enum', JSON_ARRAY('snapshot'), 'default', 'snapshot')
    ),
    'required', JSON_ARRAY('container', 'target_container'),
    'additionalProperties', true
  ),
  TRUE
);
