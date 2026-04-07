# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""AWS Glue catalog provider."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    PushResult,
    SystemType,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0


class GlueCatalogProvider:
    """CatalogProvider implementation for AWS Glue Data Catalog."""

    catalog_type: str = "AWS_GLUE"
    node_type: NodeType = NodeType.GLUE_TABLE
    system_type: SystemType = SystemType.AWS

    def __init__(self, region: str | None = None) -> None:
        self._region = region

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a GLUE_TABLE node and MATERIALIZES edge from the tableflow node."""
        glue_cfg = ci_config.get("aws_glue", ci_config)
        database = glue_cfg.get("database_name", cluster_id)
        qualified = f"glue://{database}/{topic_name}"
        glue_id = f"aws:glue_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=glue_id,
            system=SystemType.AWS,
            node_type=NodeType.GLUE_TABLE,
            qualified_name=qualified,
            display_name=f"{database}.{topic_name} (glue)",
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "catalog_type": "AWS_GLUE",
                "database": database,
                "table_name": topic_name,
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=glue_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata from AWS Glue and enrich GLUE_TABLE nodes."""
        if not self._region:
            logger.debug("Glue enrichment skipped — no region configured")
            return

        glue_nodes = graph.filter_by_type(NodeType.GLUE_TABLE)
        if not glue_nodes:
            return

        import boto3

        client = boto3.client("glue", region_name=self._region)

        for node in glue_nodes:
            if node.system != SystemType.AWS:
                continue
            await self._enrich_node(client, graph, node)

    async def _enrich_node(
        self,
        client: Any,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch metadata for a single Glue table."""
        database = node.attributes.get("database")
        table_name = node.attributes.get("table_name")
        if not database or not table_name:
            return

        for attempt in range(_MAX_RETRIES):
            try:
                response = await asyncio.to_thread(
                    client.get_table,
                    DatabaseName=database,
                    Name=table_name,
                )
                table_data = response.get("Table", {})
                storage = table_data.get("StorageDescriptor", {})
                columns = [
                    {"name": c.get("Name"), "type": c.get("Type"), "comment": c.get("Comment")}
                    for c in storage.get("Columns", [])
                ]
                partition_keys = [
                    {"name": c.get("Name"), "type": c.get("Type")}
                    for c in table_data.get("PartitionKeys", [])
                ]

                enriched_attrs = {
                    **node.attributes,
                    "aws_region": self._region,
                    "owner": table_data.get("Owner"),
                    "table_type": table_data.get("TableType"),
                    "columns": columns,
                    "partition_keys": partition_keys,
                    "storage_location": storage.get("Location"),
                    "input_format": storage.get("InputFormat"),
                    "output_format": storage.get("OutputFormat"),
                    "serde_info": storage.get("SerdeInfo", {}).get("SerializationLibrary"),
                    "parameters": table_data.get("Parameters", {}),
                    "create_time": str(table_data.get("CreateTime", "")),
                    "update_time": str(table_data.get("UpdateTime", "")),
                }
                enriched = node.model_copy(update={"attributes": enriched_attrs})
                graph.add_node(enriched)
                return

            except client.exceptions.EntityNotFoundException:
                logger.warning("Glue table %s.%s not found — skipping", database, table_name)
                return
            except client.exceptions.AccessDeniedException:
                logger.warning("Glue access denied for %s.%s — skipping", database, table_name)
                return
            except Exception:
                if attempt < _MAX_RETRIES - 1:
                    delay = _BACKOFF_BASE * (2**attempt)
                    logger.debug(
                        "Glue API error for %s.%s (attempt %d) — retrying in %.1fs",
                        database,
                        table_name,
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(
                        "Glue enrichment failed for %s.%s after %d attempts",
                        database,
                        table_name,
                        _MAX_RETRIES,
                    )

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        set_parameters: bool = True,
        set_description: bool = True,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Push Confluent lineage metadata to Glue tables via update_table API.

        Sets table parameters (key-value pairs) and description with
        upstream lineage information.
        """
        if not self._region:
            return PushResult(errors=["No AWS region configured"])

        import boto3

        client = boto3.client("glue", region_name=self._region)
        result = PushResult()

        glue_nodes = [
            n
            for n in graph.filter_by_type(NodeType.GLUE_TABLE)
            if n.system == SystemType.AWS
        ]
        if not glue_nodes:
            return result

        if on_progress:
            on_progress("Push", f"Found {len(glue_nodes)} Glue tables to update")

        now = datetime.now(UTC).isoformat()

        for node in glue_nodes:
            upstream = graph.get_upstream(node.node_id)
            if not upstream:
                continue

            database = node.attributes.get("database")
            table_name = node.attributes.get("table_name")
            if not database or not table_name:
                continue

            # Build lineage metadata
            source_topics = []
            source_connectors = []
            for up_node, _edge, _hop in upstream:
                if up_node.node_type == NodeType.KAFKA_TOPIC:
                    source_topics.append(up_node.qualified_name)
                elif up_node.node_type == NodeType.CONNECTOR:
                    source_connectors.append(up_node.display_name)

            # Fetch existing table to preserve StorageDescriptor
            try:
                existing = await asyncio.to_thread(
                    client.get_table, DatabaseName=database, Name=table_name
                )
            except Exception as exc:
                result.errors.append(f"Failed to get {database}.{table_name}: {exc}")
                continue

            table_input = self._build_table_input(existing)

            if set_parameters:
                params = table_input.setdefault("Parameters", {})
                params["lineage_bridge.source_topics"] = (
                    ",".join(source_topics) if source_topics else ""
                )
                params["lineage_bridge.source_connectors"] = (
                    ",".join(source_connectors) if source_connectors else ""
                )
                params["lineage_bridge.pipeline_type"] = "tableflow"
                params["lineage_bridge.last_synced"] = now
                params["lineage_bridge.environment_id"] = node.environment_id or ""
                params["lineage_bridge.cluster_id"] = node.cluster_id or ""

            if set_description:
                lines = []
                if source_topics:
                    topic_list = ", ".join(f'"{t}"' for t in source_topics)
                    lines.append(
                        f"Materialized from Kafka topic {topic_list} via Confluent Tableflow."
                    )
                if source_connectors:
                    conn_list = ", ".join(source_connectors)
                    lines.append(f"Sources: {conn_list}")
                if node.environment_id:
                    lines.append(f"Environment: {node.environment_id}")
                lines.append(f"Last synced: {now}")
                lines.append("Managed by LineageBridge")
                table_input["Description"] = "\n".join(lines)

            try:
                await asyncio.to_thread(
                    client.update_table,
                    DatabaseName=database,
                    TableInput=table_input,
                )
                if set_parameters:
                    result.properties_set += 1
                if set_description:
                    result.comments_set += 1
                result.tables_updated += 1
                if on_progress:
                    on_progress("Push", f"Updated {database}.{table_name}")
            except Exception as exc:
                result.errors.append(f"Update failed for {database}.{table_name}: {exc}")

        if on_progress:
            on_progress("Push", f"Done — {result.tables_updated} Glue tables updated")

        return result

    @staticmethod
    def _build_table_input(get_table_response: dict) -> dict:
        """Extract TableInput from a get_table response, preserving existing fields."""
        table = get_table_response.get("Table", {})
        # TableInput requires a subset of Table fields — exclude read-only ones
        table_input: dict[str, Any] = {}
        for key in (
            "Name",
            "Description",
            "Owner",
            "Retention",
            "StorageDescriptor",
            "PartitionKeys",
            "ViewOriginalText",
            "ViewExpandedText",
            "TableType",
            "Parameters",
            "TargetTable",
        ):
            if key in table:
                table_input[key] = table[key]
        return table_input

    def build_url(self, node: LineageNode) -> str | None:
        """Build an AWS Glue console URL for the given table node."""
        database = node.attributes.get("database")
        table_name = node.attributes.get("table_name")
        if not database or not table_name:
            return None
        region = node.attributes.get("aws_region", "us-east-1")
        return (
            f"https://{region}.console.aws.amazon.com/glue/home"
            f"?region={region}#/v2/data-catalog/tables/view/{table_name}"
            f"?database={database}"
        )
