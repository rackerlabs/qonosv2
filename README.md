# QonoS v2

QonoS v2 is a modern, extensible scheduling platform for executing time-based actions against OpenStack services. It provides cron-scheduled operations including server snapshots (Nova), volume full and incremental backups (Cinder), with Keystone authentication, trust-based delegation, retention policies, and RabbitMQ notifications.

## Architecture

QonoS consists of three main components—**API**, **scheduler**, and **worker**—that coordinate through MariaDB, with optional RabbitMQ when enabled for job wake-ups and lifecycle events.

```
     UI / CLI           ┌──────────┐                 ┌───────────┐
        │               │ Keystone │                 │ OpenStack │
        │ REST          └────▲─────┘                 └─────▲─────┘
        ▼                    │                             │
   ┌─────────┐ ─── validate ─┘                             │
   │   API   │── CRUD ──► ┌──────────────┐                 │
   └─────────┘            │   MariaDB    │                 │
                          │schedules·jobs│                 │
                          └───▲─────▲────┘                 │
                              │     │                      │
                 insert QUEUED│     │claim                 │
                              │     │                      │
                       ┌──────┴──┐  ┌┴───────┐             │
                       │Scheduler│  │ Worker │─────────────┘
                       └────┬────┘  └───┬────┘
                            │           │ job lifecycle events
                            │job_queued │
                            └─────► ┌──────────┐ ◄────┘
                                    │ RabbitMQ │
                                    └──────────┘
```

The API manages schedules and jobs. The scheduler creates QUEUED jobs when their cron schedules are due, and the worker claims and executes them against OpenStack. RabbitMQ is optional and is used to deliver job_queued wake-up notifications and lifecycle events. When AMQP is not configured, the worker continues to discover work by polling MariaDB.

## Supported action types

| Action type | Description |
|-------------|-------------|
| `server_snapshot` | Create a Glance image snapshot of a Nova server |
| `swift_object_versioning` | Swift object versioning (`mode: snapshot` copies a container into an archive) |
| `volume_backup_full` | Full Cinder backup of a block storage volume |
| `volume_backup_incremental` | Incremental Cinder backup (requires an existing successful full backup) |

Each schedule references an execution profile with trust delegation so tenant-scoped jobs run under the correct OpenStack project context.

## Requirements

- MariaDB
- OpenStack
- RabbitMQ

## Configuration

Configuration is read from `/etc/qonosv2/qonos.conf` by default. Set `QONOS_CONFIG_FILE` to use another path.

```ini
[database]
url = mysql+aiomysql://qonos:PASSWORD@mariadb-cluster-primary.openstack.svc.cluster.local:3306/qonos
pool_size = 5
max_overflow = 10
pool_recycle = 3600
pool_timeout = 30
retry_attempts = 3
retry_backoff_seconds = 0.25

[keystone]
url = http://keystone-api.openstack.svc.cluster.local:5000/v3
verify_ssl = true
username = qonos
password = PASSWORD
user_domain_name = service
project_name = service
project_domain_name = service

[openstack]
endpoint_type = internal

[amqp]
url = amqp://qonos:PASSWORD@rabbitmq.openstack.svc.cluster.local:5672/qonos
job_exchange = qonos.jobs
job_routing_key = qonos.jobs.queued
job_queue = qonos.jobs.queue
job_prefetch_count = 50
notification_exchange = qonos.events
notification_routing_key = qonos.jobs.lifecycle

[notifications]
webhook_timeout_seconds = 3.0

[scheduler]
poll_interval_seconds = 1
log_level = INFO
log_dir = /tmp
health_host = 0.0.0.0
health_port = 8082

[worker]
concurrency = 5
soft_timeout_seconds = 660
hard_timeout_seconds = 720
max_retry = 3
backoff_base_seconds = 2
backoff_max_seconds = 60
backoff_jitter = true
health_host = 0.0.0.0
health_port = 8081
stale_processing_sweep_interval_seconds = 30
stale_processing_after_seconds = 1500
idle_sleep_seconds = 1
job_cleanup_enabled = true
job_cleanup_ttl_days = 14
job_cleanup_interval_seconds = 3600
server_snapshot_poll_interval_seconds = 5
server_snapshot_poll_timeout_seconds = 600
volume_backup_poll_interval_seconds = 5
volume_backup_poll_timeout_seconds = 3600
volume_backup_coordination_wait_timeout_seconds = 300
id = auto
```

## Container image

QonoS uses a single container image for all three components: API, scheduler, and worker.

### Build and publish

Build the image locally:

```bash
docker build -f Containerfile -t qonosv2:latest .
```

Pushes to `main` publish images to `ghcr.io/rackerlabs/qonosv2` (see `.github/workflows/container-build-qonos.yaml`).

### Deployment

Deploy the same image as three Kubernetes deployments, with each deployment running a different component:

| Deployment          | Command                                               |
| ------------------- | ----------------------------------------------------- |
| **qonos-api**       | `uvicorn api.app.main:app --host 0.0.0.0 --port 8080` |
| **qonos-scheduler** | `python -m api.scheduler`                             |
| **qonos-worker**    | `python -m api.worker`                                |

All three deployments mount the same `qonos.conf` at:

```text
/etc/qonosv2/qonos.conf
```

### Database initialization

Before the first deployment, apply the database schema, typically using a db-sync init container:

```bash
alembic upgrade head
```

The initial schema is defined in:

```text
db/migrations/0001_init.sql
```

## API

The OpenAPI specification lives at [`spec/qonos-v2-api.yaml`](spec/qonos-v2-api.yaml).

Main resources (all under `/v2`, Keystone `X-Auth-Token` required):

| Resource | Endpoints |
|----------|-----------|
| Schedules | `GET/POST /schedules`, `GET/PATCH/DELETE /schedules/{id}` |
| Jobs | `GET/POST /jobs`, `GET/PATCH/DELETE /jobs/{id}` |
| Execution profiles | `GET/POST /execution-profiles`, … |
| Actions | `GET /actions` (read-only action type registry) |
| Volumes | `GET /volumes/{volume_id}/backup-chain` |

### Volume backup advisories

When both `volume_backup_full` and `volume_backup_incremental` schedules target the same volume, QonoS returns advisories recommending weekly full + daily incremental cron patterns. Advisories appear in the `X-Qonos-Advisory` response header on schedule create/update and in the backup-chain API response.

## Key concepts

### Schedules and jobs

- A **schedule** defines `action_type`, `cron_expression`, `action_parameters`, optional `retention_policy`, and an execution profile.
- The **scheduler** creates **jobs** when `next_run_at` is due, using compare-and-swap (CAS) on `next_run_at` so multiple scheduler replicas do not duplicate jobs.
- The **worker** claims `QUEUED` jobs, executes the action handler, retries on transient failure, and records faults in `job_faults`.

### Execution profiles and trust

Schedules delegate OpenStack API calls via Keystone trust. Each schedule links to an execution profile containing a `trust_id` for the target project.

### Retention

After a job completes (or fails terminally), retention runs per schedule policy (`count` or `age`):

- **Snapshots**: prunes old Glance images managed by QonoS for that schedule.
- **Full volume backups**: when `volume_backup_full` retention prunes an old full, linked incrementals on that chain are deleted too (by parent id / metadata). Unlinked incrementals are not removed.
- **Incremental volume backups**: retention applies only to incrementals from that schedule.

### Job cleanup

The worker periodically deletes expired terminal jobs (default TTL: 14 days) and their associated `job_faults` rows. Configure via `[worker] job_cleanup_*` settings.

## Observability

| Component | Health | Metrics | Default port |
|-----------|--------|---------|--------------|
| API | `/health`, `/health/live`, `/health/ready` | `/metrics` | 8080 |
| Worker | `/health` | `/metrics` | 8081 (`[worker] health_port`) |
| Scheduler | `/health` | `/metrics` | 8082 (`[scheduler] health_port`) |

## CI

Integration tests run on push/PR to `main` (see `.github/workflows/integration.yml`).