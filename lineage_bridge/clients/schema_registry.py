# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Schema Registry client — extracts schema nodes and topic-schema edges."""

from __future__ import annotations

import json
import logging
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)

# Standard TopicNameStrategy suffixes.
_SUBJECT_SUFFIXES = ("-value", "-key")


def _topic_from_subject(subject: str) -> tuple[str, str] | None:
    """Derive ``(topic_name, key_or_value)`` from a subject string.

    Returns ``None`` if the subject does not match the standard convention.
    """
    for suffix in _SUBJECT_SUFFIXES:
        if subject.endswith(suffix):
            return subject[: -len(suffix)], suffix.lstrip("-")
    return None


class SchemaRegistryClient(ConfluentClient):
    """Extracts schema lineage from the Confluent Schema Registry."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        environment_id: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.environment_id = environment_id

    # ── helpers ─────────────────────────────────────────────────────────

    def _schema_node_id(self, subject: str) -> str:
        return f"confluent:schema:{self.environment_id}:{subject}"

    def _topic_node_id(self, topic: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic}"

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        subjects = await self._list_subjects()
        logger.debug("SchemaRegistry found %d subjects", len(subjects))

        for subject in subjects:
            try:
                schema_info = await self._get_latest_schema(subject)
            except Exception:
                logger.warning("Failed to fetch schema for subject %s", subject, exc_info=True)
                continue

            schema_type = schema_info.get("schemaType", "AVRO")
            schema_str = schema_info.get("schema", "")

            # Parse fields once; count + structured list are both derived.
            fields = self._parse_fields(schema_str, schema_type)
            field_count = len(fields) if fields is not None else None

            nodes.append(
                LineageNode(
                    node_id=self._schema_node_id(subject),
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.SCHEMA,
                    qualified_name=subject,
                    display_name=subject,
                    environment_id=self.environment_id,
                    attributes={
                        "schema_type": schema_type,
                        "version": schema_info.get("version"),
                        "schema_id": schema_info.get("id"),
                        "field_count": field_count,
                        "fields": fields or [],
                        "schema_string": schema_str,
                    },
                )
            )

            # Derive topic → schema edge
            parsed = _topic_from_subject(subject)
            if parsed:
                topic_name, key_or_value = parsed
                tid = self._topic_node_id(topic_name)
                sid = self._schema_node_id(subject)
                edges.append(
                    LineageEdge(
                        src_id=tid,
                        dst_id=sid,
                        edge_type=EdgeType.HAS_SCHEMA,
                        attributes={"role": key_or_value},
                    )
                )

        logger.info("SchemaRegistry extracted %d nodes, %d edges", len(nodes), len(edges))
        return nodes, edges

    # ── field parsing ───────────────────────────────────────────────────

    @staticmethod
    def _parse_fields(schema_str: str, schema_type: str) -> list[dict[str, str]] | None:
        """Best-effort structured field list from the raw schema string.

        Returns a list of ``{name, type, description?}`` dicts, or ``None``
        if the schema can't be parsed (e.g. PROTOBUF, malformed JSON).
        Consumed by catalog providers that surface schema metadata.
        """
        if not schema_str:
            return None
        try:
            if schema_type == "AVRO":
                parsed = json.loads(schema_str)
                out: list[dict[str, str]] = []
                for f in parsed.get("fields", []):
                    if not isinstance(f, dict) or not f.get("name"):
                        continue
                    entry: dict[str, str] = {"name": str(f["name"])}
                    if f.get("type") is not None:
                        entry["type"] = (
                            json.dumps(f["type"])
                            if isinstance(f["type"], (dict, list))
                            else str(f["type"])
                        )
                    doc = f.get("doc") or f.get("description")
                    if doc:
                        entry["description"] = str(doc)
                    out.append(entry)
                return out
            if schema_type == "JSON":
                parsed = json.loads(schema_str)
                props = parsed.get("properties", {})
                if not isinstance(props, dict) or not props:
                    return None
                out_json: list[dict[str, str]] = []
                for name, spec in props.items():
                    entry = {"name": str(name)}
                    if isinstance(spec, dict):
                        if spec.get("type") is not None:
                            entry["type"] = (
                                json.dumps(spec["type"])
                                if isinstance(spec["type"], (list, dict))
                                else str(spec["type"])
                            )
                        desc = spec.get("description")
                        if desc:
                            entry["description"] = str(desc)
                    out_json.append(entry)
                return out_json
        except Exception:
            pass
        return None

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_subjects(self) -> list[str]:
        data = await self.get("/subjects")
        if isinstance(data, list):
            return data
        return []

    async def _get_latest_schema(self, subject: str) -> dict[str, Any]:
        return await self.get(f"/subjects/{subject}/versions/latest")
