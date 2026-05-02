# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Rewrite OpenLineage dataset namespaces into formats native catalogs accept.

LineageBridge's internal namespaces (``confluent://env/cluster``,
``google://project/dataset``, ``aws://region/db``, ``databricks://workspace``)
are unrecognised by external lineage processors — they only accept the
"FQN-able" formats from the OpenLineage naming spec.

This module is the single source of truth for that mapping. Used by both
:class:`GoogleLineageProvider` and :class:`AWSDataZoneProvider` (via
parametrized ``allowlist``).
"""

from __future__ import annotations

from typing import Any


def kafka_fqn(cluster_id: str, topic: str) -> str:
    """Build the FQN that ``processOpenLineageRunEvent`` derives from a Kafka topic.

    Google escapes topic names containing ``.`` with backticks when normalising
    a ``kafka://<host>`` namespace + ``<topic>`` name pair. We mirror that so
    catalog entries we register match exactly.
    """
    if "." in topic or " " in topic:
        return f"kafka:{cluster_id}.`{topic}`"
    return f"kafka:{cluster_id}.{topic}"


def normalize_event(event: Any, *, allow: set[str]) -> None:
    """Rewrite dataset namespaces in-place so the configured allowlist accepts them.

    ``allow`` is the set of *output* namespaces the target processor recognises
    (e.g. ``{"bigquery"}`` for Google, ``{"aws"}`` for DataZone Glue).
    Datasets in unrecognised namespaces are dropped from the event since the
    receiving catalog can't link them either.

    Mappings applied to both ``inputs`` and ``outputs``:
    - ``confluent://env/cluster`` → ``kafka://<cluster>``
    - ``kafka://*`` → preserved
    - ``google://project/dataset`` → ``bigquery`` (output side only)
    - ``aws://region/database`` → ``aws://*`` (output side only — DataZone)
    - everything else → dropped
    """
    event.inputs = [ds for ds in (_rewrite_input(ds) for ds in event.inputs) if ds is not None]
    event.outputs = [
        ds for ds in (_rewrite_output(ds, allow=allow) for ds in event.outputs) if ds is not None
    ]


def _rewrite_input(ds: Any) -> Any | None:
    ns = ds.namespace or ""
    if ns.startswith("confluent://"):
        ds.namespace = f"kafka://{ns.rsplit('/', 1)[-1] or 'unknown'}"
        return ds
    if ns.startswith("kafka://") or ns == "bigquery":
        return ds
    return None


def _rewrite_output(ds: Any, *, allow: set[str]) -> Any | None:
    ns = ds.namespace or ""
    # Confluent → kafka (Flink/ksqlDB intermediate writes back to Kafka).
    if ns.startswith("confluent://"):
        ds.namespace = f"kafka://{ns.rsplit('/', 1)[-1] or 'unknown'}"
        return ds
    if ns.startswith("kafka://"):
        return ds
    if "bigquery" in allow and (ns.startswith("google://") or ns == "bigquery"):
        ds.namespace = "bigquery"
        return ds
    if "aws" in allow and ns.startswith("aws://"):
        return ds
    return None
