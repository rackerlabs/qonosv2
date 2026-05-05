from __future__ import annotations

from api.app.notifications import JOB_EVENT_TYPES, build_job_event_payload


def test_build_job_event_payload_for_each_event_type() -> None:
    for event_type in sorted(JOB_EVENT_TYPES):
        payload = build_job_event_payload(
            event_type=event_type,
            job_id="job-1",
            schedule_id="schedule-1",
            project_id="project-1",
            action_type="server_snapshot",
            status="QUEUED",
            error_message="boom",
        )
        assert payload["event_type"] == event_type
        assert payload["job_id"] == "job-1"
        assert payload["schedule_id"] == "schedule-1"
        assert payload["project_id"] == "project-1"
        assert payload["action_type"] == "server_snapshot"
        assert payload["status"] == "QUEUED"
        assert payload["error_message"] == "boom"
        assert payload["timestamp"]


def test_build_job_event_payload_rejects_unknown_event() -> None:
    try:
        build_job_event_payload(
            event_type="job.unknown",
            job_id="job-1",
            schedule_id="schedule-1",
            project_id="project-1",
            action_type="server_snapshot",
            status="QUEUED",
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "Unsupported job event type" in str(exc)
