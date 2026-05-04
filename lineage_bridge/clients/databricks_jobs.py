# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks Jobs API client.

Used by `catalogs/databricks_uc.py` to enrich NOTEBOOK nodes with the
metadata of the job that owns them — surfaced via `workflowInfos.job_id`
in the lineage-tracking API response. Two read-only calls per unique
job_id, both gracefully handling 404 (job deleted) / 403 (permission).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass
class JobInfo:
    """Subset of Databricks job metadata we attach to NOTEBOOK nodes."""

    job_id: int
    name: str | None = None
    schedule_cron: str | None = None
    schedule_timezone: str | None = None
    schedule_paused: bool | None = None
    creator: str | None = None
    # First notebook_task path discovered in the job's task list. Lets the
    # UI show the actual notebook name + deeplink by path (more stable than
    # the numeric notebook_id that the lineage API gives us).
    notebook_path: str | None = None


@dataclass
class RunInfo:
    """Most recent run of a job — life cycle + result state + start time."""

    run_id: int
    life_cycle_state: str | None = None
    result_state: str | None = None
    start_time_ms: int | None = None
    state_message: str | None = None


async def get_job(client: httpx.AsyncClient, job_id: int) -> JobInfo | None:
    """Fetch a job's settings via ``/api/2.1/jobs/get``.

    Returns ``None`` on 404 / 403 / unexpected error so callers can carry
    on enriching other notebooks. The lineage walk that produced the
    job_id may have been against a workspace where the principal can read
    table lineage but not job settings.
    """
    try:
        resp = await client.get("/api/2.1/jobs/get", params={"job_id": job_id})
    except httpx.HTTPError as exc:
        logger.debug("Jobs get %s failed: %s", job_id, exc)
        return None

    if resp.status_code != 200:
        logger.debug("Jobs get %s returned %d", job_id, resp.status_code)
        return None

    data = resp.json()
    settings = data.get("settings") or {}
    schedule = settings.get("schedule") or {}
    notebook_path = None
    for task in settings.get("tasks") or []:
        nb_task = task.get("notebook_task") or {}
        if nb_task.get("notebook_path"):
            notebook_path = nb_task["notebook_path"]
            break
    return JobInfo(
        job_id=job_id,
        name=settings.get("name"),
        schedule_cron=schedule.get("quartz_cron_expression"),
        schedule_timezone=schedule.get("timezone_id"),
        schedule_paused=(schedule.get("pause_status") == "PAUSED")
        if schedule.get("pause_status")
        else None,
        creator=data.get("creator_user_name"),
        notebook_path=notebook_path,
    )


async def get_last_run(client: httpx.AsyncClient, job_id: int) -> RunInfo | None:
    """Fetch the most recent run of ``job_id`` via ``/api/2.1/jobs/runs/list``.

    Limit=1 so the response is small even on jobs with thousands of runs.
    Returns ``None`` if the API call fails or the job has never run.
    """
    try:
        resp = await client.get(
            "/api/2.1/jobs/runs/list",
            params={"job_id": job_id, "limit": 1, "expand_tasks": "false"},
        )
    except httpx.HTTPError as exc:
        logger.debug("Jobs runs/list %s failed: %s", job_id, exc)
        return None

    if resp.status_code != 200:
        logger.debug("Jobs runs/list %s returned %d", job_id, resp.status_code)
        return None

    runs = resp.json().get("runs") or []
    if not runs:
        return None
    run = runs[0]
    state = run.get("state") or {}
    return RunInfo(
        run_id=run.get("run_id"),
        life_cycle_state=state.get("life_cycle_state"),
        result_state=state.get("result_state"),
        start_time_ms=run.get("start_time"),
        state_message=state.get("state_message"),
    )
