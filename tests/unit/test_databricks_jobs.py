# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Databricks Jobs API client."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.databricks_jobs import (
    JobInfo,
    RunInfo,
    get_job,
    get_last_run,
)

WORKSPACE_URL = "https://acme-prod.cloud.databricks.com"
JOBS_GET = f"{WORKSPACE_URL}/api/2.1/jobs/get"
RUNS_LIST = f"{WORKSPACE_URL}/api/2.1/jobs/runs/list"


@pytest.fixture()
async def client():
    async with httpx.AsyncClient(base_url=WORKSPACE_URL) as c:
        yield c


class TestGetJob:
    @respx.mock
    async def test_returns_job_info_with_schedule(self, client):
        respx.get(JOBS_GET, params={"job_id": "42"}).mock(
            return_value=httpx.Response(
                200,
                json={
                    "job_id": 42,
                    "creator_user_name": "ops@example.com",
                    "settings": {
                        "name": "customer_order_summary",
                        "schedule": {
                            "quartz_cron_expression": "0 0/5 * ? * * *",
                            "timezone_id": "UTC",
                            "pause_status": "UNPAUSED",
                        },
                    },
                },
            )
        )
        info = await get_job(client, 42)
        assert info == JobInfo(
            job_id=42,
            name="customer_order_summary",
            schedule_cron="0 0/5 * ? * * *",
            schedule_timezone="UTC",
            schedule_paused=False,
            creator="ops@example.com",
        )

    @respx.mock
    async def test_extracts_notebook_path_from_first_notebook_task(self, client):
        """The first notebook_task.notebook_path is surfaced so the UI can
        show the notebook name + build a path-based deeplink."""
        respx.get(JOBS_GET, params={"job_id": "55"}).mock(
            return_value=httpx.Response(
                200,
                json={
                    "job_id": 55,
                    "settings": {
                        "name": "summarize",
                        "tasks": [
                            {
                                "task_key": "summarize",
                                "notebook_task": {
                                    "notebook_path": "/Shared/lb-uc/customer_order_summary"
                                },
                            }
                        ],
                    },
                },
            )
        )
        info = await get_job(client, 55)
        assert info is not None
        assert info.notebook_path == "/Shared/lb-uc/customer_order_summary"

    @respx.mock
    async def test_no_notebook_task_leaves_path_none(self, client):
        """Pure SQL/JAR jobs have no notebook_task — notebook_path stays None."""
        respx.get(JOBS_GET, params={"job_id": "56"}).mock(
            return_value=httpx.Response(
                200,
                json={
                    "job_id": 56,
                    "settings": {
                        "name": "sql-job",
                        "tasks": [{"task_key": "q", "sql_task": {"file": {"path": "/foo.sql"}}}],
                    },
                },
            )
        )
        info = await get_job(client, 56)
        assert info is not None
        assert info.notebook_path is None

    @respx.mock
    async def test_unscheduled_job_omits_schedule(self, client):
        respx.get(JOBS_GET, params={"job_id": "7"}).mock(
            return_value=httpx.Response(
                200,
                json={"job_id": 7, "settings": {"name": "ad-hoc"}},
            )
        )
        info = await get_job(client, 7)
        assert info is not None
        assert info.name == "ad-hoc"
        assert info.schedule_cron is None
        assert info.schedule_paused is None

    @respx.mock
    async def test_paused_job_marks_paused_true(self, client):
        respx.get(JOBS_GET, params={"job_id": "8"}).mock(
            return_value=httpx.Response(
                200,
                json={
                    "job_id": 8,
                    "settings": {
                        "name": "paused-job",
                        "schedule": {
                            "quartz_cron_expression": "0 * * * * ? *",
                            "timezone_id": "UTC",
                            "pause_status": "PAUSED",
                        },
                    },
                },
            )
        )
        info = await get_job(client, 8)
        assert info is not None and info.schedule_paused is True

    @respx.mock
    async def test_404_returns_none(self, client):
        """Job deleted between lineage walk and enrichment — caller carries on."""
        respx.get(JOBS_GET, params={"job_id": "999"}).mock(
            return_value=httpx.Response(404, json={"error_code": "RESOURCE_DOES_NOT_EXIST"})
        )
        assert await get_job(client, 999) is None

    @respx.mock
    async def test_403_returns_none(self, client):
        """Principal lacks job-read permission — degrade gracefully."""
        respx.get(JOBS_GET, params={"job_id": "10"}).mock(
            return_value=httpx.Response(403, json={"error_code": "PERMISSION_DENIED"})
        )
        assert await get_job(client, 10) is None

    @respx.mock
    async def test_network_error_returns_none(self, client):
        respx.get(JOBS_GET).mock(side_effect=httpx.ConnectError("boom"))
        assert await get_job(client, 1) is None


class TestGetLastRun:
    @respx.mock
    async def test_returns_most_recent_run(self, client):
        respx.get(RUNS_LIST, params={"job_id": "42", "limit": "1", "expand_tasks": "false"}).mock(
            return_value=httpx.Response(
                200,
                json={
                    "runs": [
                        {
                            "run_id": 555,
                            "state": {
                                "life_cycle_state": "TERMINATED",
                                "result_state": "SUCCESS",
                                "state_message": "",
                            },
                            "start_time": 1234567890123,
                        }
                    ]
                },
            )
        )
        info = await get_last_run(client, 42)
        assert info == RunInfo(
            run_id=555,
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
            start_time_ms=1234567890123,
            state_message="",
        )

    @respx.mock
    async def test_no_runs_returns_none(self, client):
        """Job exists but has never run — return None, don't fabricate a row."""
        respx.get(RUNS_LIST, params={"job_id": "42", "limit": "1", "expand_tasks": "false"}).mock(
            return_value=httpx.Response(200, json={})
        )
        assert await get_last_run(client, 42) is None

    @respx.mock
    async def test_empty_runs_returns_none(self, client):
        respx.get(RUNS_LIST, params={"job_id": "42", "limit": "1", "expand_tasks": "false"}).mock(
            return_value=httpx.Response(200, json={"runs": []})
        )
        assert await get_last_run(client, 42) is None

    @respx.mock
    async def test_404_returns_none(self, client):
        respx.get(RUNS_LIST).mock(return_value=httpx.Response(404, json={}))
        assert await get_last_run(client, 1) is None
