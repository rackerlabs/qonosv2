-- QonoS v2 persistence: initial schema

-- Migration bookkeeping
CREATE TABLE IF NOT EXISTS schema_migrations (
  version VARCHAR(64) NOT NULL PRIMARY KEY,
  applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Action type registry
CREATE TABLE IF NOT EXISTS action_types (
  action_type VARCHAR(100) NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT NULL,
  action_parameters_schema JSON NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Execution profiles
CREATE TABLE IF NOT EXISTS execution_profiles (
  id CHAR(36) NOT NULL PRIMARY KEY,
  project_id CHAR(36) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT NULL,
  auth_type VARCHAR(32) NOT NULL DEFAULT 'trust',
  trust_id CHAR(36) NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  deleted_at DATETIME(6) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX execution_profiles_project_id_idx ON execution_profiles(project_id);
CREATE UNIQUE INDEX execution_profiles_project_name_uniq ON execution_profiles(project_id, name);

-- Schedules
CREATE TABLE IF NOT EXISTS schedules (
  id CHAR(36) NOT NULL PRIMARY KEY,
  project_id CHAR(36) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT NULL,
  action_type VARCHAR(100) NOT NULL,
  cron_expression VARCHAR(255) NOT NULL,
  action_parameters JSON NOT NULL,
  retention_policy JSON NULL,
  execution_profile_id CHAR(36) NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  next_run_at DATETIME(6) NULL,
  last_run_at DATETIME(6) NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  deleted_at DATETIME(6) NULL,

  active_name VARCHAR(255) AS (IF(deleted_at IS NULL, name, NULL)) STORED,

  CONSTRAINT schedules_action_type_fk
    FOREIGN KEY (action_type) REFERENCES action_types(action_type),
  CONSTRAINT schedules_execution_profile_fk
    FOREIGN KEY (execution_profile_id) REFERENCES execution_profiles(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE UNIQUE INDEX schedules_project_active_name_uniq ON schedules(project_id, active_name);
CREATE INDEX schedules_project_id_idx ON schedules(project_id);
CREATE INDEX schedules_project_execution_profile_idx ON schedules(project_id, execution_profile_id);
CREATE INDEX schedules_next_run_at_idx ON schedules(next_run_at);

-- Jobs
CREATE TABLE IF NOT EXISTS jobs (
  id CHAR(36) NOT NULL PRIMARY KEY,
  schedule_id CHAR(36) NOT NULL,
  project_id CHAR(36) NOT NULL,
  action_type VARCHAR(100) NOT NULL,
  action_parameters JSON NOT NULL,
  run_at DATETIME(6) NOT NULL,
  status ENUM(
    'QUEUED',
    'PROCESSING',
    'DONE',
    'TIMED_OUT',
    'ERROR',
    'CANCELLED',
    'HARD_TIMED_OUT',
    'MAX_RETRIED'
  ) NOT NULL DEFAULT 'QUEUED',
  version INT NOT NULL DEFAULT 1,
  started_at DATETIME(6) NULL,
  ended_at DATETIME(6) NULL,
  retry_count INT NOT NULL DEFAULT 0,
  error_message TEXT NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  cancelled_at DATETIME(6) NULL,
  completed_at DATETIME(6) NULL,

  CONSTRAINT jobs_schedule_fk
    FOREIGN KEY (schedule_id) REFERENCES schedules(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX jobs_project_id_run_at_idx ON jobs(project_id, run_at);
CREATE INDEX jobs_schedule_id_run_at_idx ON jobs(schedule_id, run_at);
CREATE INDEX jobs_status_idx ON jobs(status);

-- Job faults
CREATE TABLE IF NOT EXISTS job_faults (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_id CHAR(36) NOT NULL,
  project_id CHAR(36) NOT NULL,
  occurred_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  code VARCHAR(100) NOT NULL,
  title VARCHAR(255) NOT NULL,
  detail TEXT NOT NULL,
  extra JSON NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  CONSTRAINT job_faults_job_fk
    FOREIGN KEY (job_id) REFERENCES jobs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX job_faults_job_id_idx ON job_faults(job_id, occurred_at);
CREATE INDEX job_faults_project_id_idx ON job_faults(project_id, occurred_at);

