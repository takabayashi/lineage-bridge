"""Stream Catalog (Stream Governance) client — enriches existing nodes."""

from __future__ import annotations

import logging
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
)

logger = logging.getLogger(__name__)


class StreamCatalogClient(ConfluentClient):
    """Enriches existing graph nodes with tags, owners, and descriptions
    from the Confluent Stream Catalog API.

    This extractor does **not** produce new nodes or edges via ``extract()``.
    Instead, call :meth:`enrich` with an already-populated graph.
    """

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

    # ── LineageExtractor protocol (no-op) ───────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Return empty results — enrichment happens via :meth:`enrich`."""
        return [], []

    # ── enrichment ──────────────────────────────────────────────────────

    async def enrich(self, graph: LineageGraph) -> None:
        """Update existing nodes in *graph* with catalog metadata.

        Queries the catalog for ``kafka_topic`` and ``kafka_connector``
        entities and merges tags, owner, and description into matching nodes.
        """
        enriched = 0

        for entity_type, node_type in [
            ("kafka_topic", NodeType.KAFKA_TOPIC),
            ("kafka_connector", NodeType.CONNECTOR),
        ]:
            try:
                entities = await self._search_entities(entity_type)
            except Exception:
                logger.warning("Failed to search catalog for %s", entity_type, exc_info=True)
                continue

            for entity in entities:
                attrs = entity.get("attributes", {})
                name = attrs.get("name", "")
                if not name:
                    continue

                # Build the node ID the same way KafkaAdmin / Connect do.
                node_id = f"confluent:{node_type.value}:{self.environment_id}:{name}"
                node = graph.get_node(node_id)
                if node is None:
                    continue

                # Merge enrichment data.
                owner = attrs.get("owner")
                description = attrs.get("description")
                classifications = entity.get("classificationNames", [])

                if owner:
                    node.attributes["owner"] = owner
                if description:
                    node.attributes["description"] = description
                if classifications:
                    node.tags = list(set(node.tags + classifications))

                enriched += 1

        logger.info(
            "StreamCatalog enriched %d nodes in environment %s",
            enriched,
            self.environment_id,
        )

    # ── raw API calls ───────────────────────────────────────────────────

    async def _search_entities(self, entity_type: str, limit: int = 500) -> list[dict[str, Any]]:
        """Paginate through ``/catalog/v1/search/basic`` for *entity_type*."""
        all_results: list[dict[str, Any]] = []
        offset = 0

        while True:
            data = await self.get(
                "/catalog/v1/search/basic",
                params={
                    "type": entity_type,
                    "attrs": "owner,description",
                    "limit": limit,
                    "offset": offset,
                },
            )
            results = data.get("searchResults", [])
            all_results.extend(results)
            if len(results) < limit:
                break
            offset += limit

        return all_results
