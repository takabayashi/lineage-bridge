# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Confluent Cloud audit log event model and lineage relevance filter."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── Lineage-relevant audit event methods ───────────────────────────────────

LINEAGE_RELEVANT_METHODS: set[str] = {
    # Kafka topic lifecycle
    "kafka.CreateTopics",
    "kafka.DeleteTopics",
    "kafka.IncrementalAlterConfigs",
    # Connect lifecycle
    "connect.CreateConnector",
    "connect.DeleteConnector",
    "connect.AlterConnector",
    "connect.PauseConnector",
    "connect.ResumeConnector",
}

LINEAGE_RELEVANT_PREFIXES: tuple[str, ...] = (
    "ksql.",
    "io.confluent.flink.",
    "io.confluent.schema_registry.",
)

# CRN patterns: crn://confluent.cloud/organization=xxx/environment=env-xxx/...
_ENV_RE = re.compile(r"/environment=(env-[a-z0-9]+)")
_CLUSTER_RE = re.compile(r"/kafka=(lkc-[a-z0-9]+)")


def is_lineage_relevant(method_name: str) -> bool:
    """Check if an audit log method name indicates a lineage-relevant change."""
    if method_name in LINEAGE_RELEVANT_METHODS:
        return True
    return any(method_name.startswith(p) for p in LINEAGE_RELEVANT_PREFIXES)


def _extract_from_crn(crn: str, pattern: re.Pattern[str]) -> str | None:
    """Extract an ID from a Confluent Resource Name."""
    m = pattern.search(crn)
    return m.group(1) if m else None


class AuditEvent(BaseModel):
    """Parsed Confluent Cloud audit log event."""

    id: str
    time: datetime
    method_name: str
    resource_name: str
    principal: str
    environment_id: str | None = None
    cluster_id: str | None = None
    raw: dict[str, Any]

    @classmethod
    def from_cloud_event(cls, payload: dict[str, Any]) -> AuditEvent | None:
        """Parse a CloudEvent JSON payload into an AuditEvent.

        Returns None if the payload is malformed or missing required fields.
        """
        try:
            data = payload.get("data", {})
            method_name = data.get("methodName", "")
            if not method_name:
                return None

            source = payload.get("source", "")
            resource_name = data.get("resourceName", "")
            # Extract IDs from CRN strings (source or resourceName)
            crn = source or resource_name
            environment_id = _extract_from_crn(crn, _ENV_RE)
            cluster_id = _extract_from_crn(crn, _CLUSTER_RE)

            # Extract principal (the actor who made the change)
            auth_info = data.get("authenticationInfo", {})
            principal = auth_info.get("principal", "unknown")

            time_str = payload.get("time", "")
            if not time_str:
                return None

            return cls(
                id=payload.get("id", ""),
                time=datetime.fromisoformat(time_str.replace("Z", "+00:00")),
                method_name=method_name,
                resource_name=resource_name,
                principal=principal,
                environment_id=environment_id,
                cluster_id=cluster_id,
                raw=payload,
            )
        except Exception:
            logger.debug("Failed to parse audit event", exc_info=True)
            return None
