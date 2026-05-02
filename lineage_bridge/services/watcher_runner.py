# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Watcher runner — long-running asyncio loop that drives a `WatcherService`.

The runner is the only piece that owns I/O: it asks the service to poll, the
service yields events + status, the runner persists everything to a
`WatcherRepository` and sleeps until the next tick.

Two callers use this:

  - `api/routers/watcher.py` (in-process) — `POST /api/v1/watcher/start`
    creates a `WatcherRunner` and schedules `runner.run_forever()` as an
    asyncio task on the API event loop. `POST /stop` flips the stop event.
  - `watcher/cli.py` (out-of-process daemon) — runs `run_forever` in its own
    event loop, registers itself in the same storage backend so the API can
    see the daemon's state.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

from lineage_bridge.services.watcher_models import WatcherConfig
from lineage_bridge.services.watcher_service import WatcherService

if TYPE_CHECKING:
    from lineage_bridge.config.settings import Settings
    from lineage_bridge.storage.protocol import WatcherRepository

logger = logging.getLogger(__name__)


class WatcherRunner:
    """One runner per active watcher. Owns the asyncio loop tick + persistence."""

    def __init__(
        self,
        watcher_id: str,
        config: WatcherConfig,
        settings: Settings,
        repo: WatcherRepository,
    ) -> None:
        self.watcher_id = watcher_id
        self.config = config
        self.settings = settings
        self.repo = repo
        self._service = WatcherService(watcher_id, config, settings)
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    @classmethod
    def spawn(
        cls,
        config: WatcherConfig,
        settings: Settings,
        repo: WatcherRepository,
    ) -> WatcherRunner:
        """Generate a fresh watcher_id, register the config, return the runner.

        Writes an initial WATCHING status row before returning so that callers
        which immediately query `GET /watcher/{id}/status` after `POST /watcher`
        see a row instead of a 404 (the runner's own first `_persist_status`
        happens inside the asyncio task body, which may not have run yet).

        The persisted config has audit-log + per-cluster + per-env credentials
        stripped — the runner re-fetches them from `Settings` at start time
        (see `WatcherService._build_audit_consumer` / `_build_rest_poller`).
        Without this strip, a `storage.db` backup or `sqlite3 .dump` would
        disclose Confluent / Kafka credentials in plaintext.

        The caller is responsible for `await runner.start()` (in the API
        process) or `await runner.run_forever()` (in the daemon).
        """
        from datetime import UTC, datetime

        from lineage_bridge.services.watcher_models import WatcherState, WatcherStatus

        watcher_id = uuid.uuid4().hex
        repo.register(watcher_id, _strip_secrets(config))
        repo.update_status(
            watcher_id,
            WatcherStatus(
                watcher_id=watcher_id,
                state=WatcherState.WATCHING,
                started_at=datetime.now(UTC),
            ),
        )
        # In-memory runner keeps the un-stripped config so it can authenticate
        # against the data plane on the very first poll without re-reading
        # from Settings (which it does anyway as a fallback).
        return cls(watcher_id, config, settings, repo)

    async def start(self) -> None:
        """Start the runner as a background asyncio task. Returns immediately.

        Used by the API router so the HTTP request can return the watcher_id
        without blocking on the (long-lived) loop.
        """
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self.run_forever(), name=f"watcher-{self.watcher_id}")

    async def stop(self) -> None:
        """Signal the loop to stop on its next tick. Awaits the task to drain."""
        self._stop_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except TimeoutError:
                self._task.cancel()
                logger.warning("Watcher %s did not stop in 10s; cancelled", self.watcher_id)
            self._task = None

    async def run_forever(self) -> None:
        """The poll → maybe-extract → persist loop. Returns when stop_event is set."""
        try:
            await self._service.start()
            self._persist_status()

            while not self._stop_event.is_set():
                # 1. Detect changes (poll REST or read one Kafka event).
                events = await self._service.poll_once()
                for event in events:
                    self.repo.append_event(self.watcher_id, event)

                # 2. Trigger extraction if cooldown elapsed.
                record = await self._service.maybe_extract()
                if record is not None:
                    self.repo.append_extraction(self.watcher_id, record)

                # 3. Persist the latest status snapshot every tick.
                self._persist_status()

                # 4. Sleep until the next tick — interruptible via stop_event.
                # Shorter wait in audit-log mode (consumer already blocks 1s)
                # so the stop signal is responsive.
                wait_for = (
                    0.1
                    if self.config.mode.value == "audit_log"
                    else self.config.poll_interval_seconds
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=wait_for)
        except Exception:
            logger.exception("Watcher %s run loop crashed", self.watcher_id)
        finally:
            await self._service.stop()
            # Final status persist so the UI sees the STOPPED state.
            self._persist_status()
            logger.info("Watcher %s run_forever exited", self.watcher_id)

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _persist_status(self) -> None:
        self.repo.update_status(self.watcher_id, self._service.snapshot())


async def run_forever_blocking(
    config: WatcherConfig,
    settings: Settings,
    repo: WatcherRepository,
) -> str:
    """Daemon-mode entry point: register a watcher, run its loop until SIGINT.

    Returns the assigned `watcher_id`. The CLI uses this; the loop runs until
    the caller breaks out via Ctrl+C / SIGTERM (handled by the CLI wrapper).
    """
    runner = WatcherRunner.spawn(config, settings, repo)
    print(f"Watcher started: {runner.watcher_id}")
    await runner.run_forever()
    return runner.watcher_id


def _strip_secrets(config: WatcherConfig) -> WatcherConfig:
    """Return a copy of *config* with all credential fields blanked.

    The runner re-injects them from `Settings` at start time. This keeps
    secrets out of the persisted-config row in `storage.db` (and out of
    the JSON the API serialises in `GET /api/v1/watcher`), so a backup
    file or read-only DB dump doesn't leak Confluent / Kafka creds.
    """
    return config.model_copy(
        update={
            "audit_log_api_key": None,
            "audit_log_api_secret": None,
            "extraction": config.extraction.model_copy(
                update={
                    "sr_credentials": {},
                    "flink_credentials": {},
                    "cluster_credentials": {},
                }
            ),
        }
    )


def stop_event_for(runner: WatcherRunner) -> asyncio.Event:
    """Test-only helper: expose the runner's stop event for unit tests that
    can't await `runner.stop()` because they construct the runner outside
    an event loop. Also useful when wrapping the runner in a custom signal
    handler in the CLI."""
    return runner._stop_event
