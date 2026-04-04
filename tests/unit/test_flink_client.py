"""Unit tests for lineage_bridge.clients.flink.FlinkClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.flink import FlinkClient
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType

# ── shared constants ──────────────────────────────────────────────────────

CLOUD_KEY = "cloud-key"
CLOUD_SECRET = "cloud-secret"
ENV_ID = "env-abc123"
ORG_ID = "org-abc123"
CLOUD_URL = "https://api.confluent.cloud"
FLINK_URL = "https://flink.us-east-1.aws.confluent.cloud"


@pytest.fixture()
def flink_client():
    return FlinkClient(
        cloud_api_key=CLOUD_KEY,
        cloud_api_secret=CLOUD_SECRET,
        environment_id=ENV_ID,
        organization_id=ORG_ID,
        cloud_base_url=CLOUD_URL,
        timeout=5.0,
    )


# ── FlinkClient._parse_flink_sql tests ───────────────────────────────────────────────


class TestParseFlinkSql:
    """Tests for the static SQL parser."""

    def test_insert_into_with_from(self):
        sql = (
            "INSERT INTO enriched_orders "
            "SELECT o.order_id, c.name "
            "FROM orders o JOIN customers c ON o.customer_id = c.id"
        )
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"enriched_orders"}
        assert sources == {"orders", "customers"}

    def test_backtick_table_names(self):
        sql = "INSERT INTO `my-sink` SELECT * FROM `my-source`"
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"my-sink"}
        assert sources == {"my-source"}

    def test_three_part_names_uses_last_segment(self):
        sql = "INSERT INTO env.cluster.sink_topic SELECT * FROM env.cluster.source_topic"
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"sink_topic"}
        assert sources == {"source_topic"}

    def test_noise_keywords_filtered(self):
        """SQL noise like SELECT, WHERE should not appear in sources."""
        sql = "INSERT INTO output SELECT * FROM input WHERE 1=1 GROUP BY key"
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert "select" not in {s.lower() for s in sources}
        assert "where" not in {s.lower() for s in sources}
        assert "group" not in {s.lower() for s in sources}
        assert sources == {"input"}
        assert sinks == {"output"}

    def test_sink_not_in_sources(self):
        """A table that is both INSERT INTO and FROM should only be a sink."""
        sql = "INSERT INTO orders SELECT * FROM orders"
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"orders"}
        assert "orders" not in sources


# ── extract() tests ──────────────────────────────────────────────────────


class TestFlinkExtract:

    @respx.mock
    async def test_extract_no_compute_pools_returns_empty(self, flink_client, monkeypatch):
        """When there are no compute pools, extract returns empty lists."""
        monkeypatch.setattr("asyncio.sleep", _no_sleep)

        respx.get(f"{CLOUD_URL}/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {"next": None}})
        )

        nodes, edges = await flink_client.extract()
        assert nodes == []
        assert edges == []

    @respx.mock
    async def test_extract_with_statements(self, flink_client, monkeypatch):
        """Full extract: discovers pools, fetches statements, builds lineage."""
        monkeypatch.setattr("asyncio.sleep", _no_sleep)

        # 1) Compute pools response
        pools_resp = {
            "data": [
                {
                    "id": "lfcp-abc123",
                    "spec": {
                        "region": "us-east-1",
                        "cloud": "AWS",
                    },
                }
            ],
            "metadata": {"next": None},
        }
        respx.get(f"{CLOUD_URL}/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(200, json=pools_resp)
        )

        # 2) Flink statements response
        stmt_path = (
            f"/sql/v1/organizations/{ORG_ID}"
            f"/environments/{ENV_ID}/statements"
        )
        statements_resp = {
            "data": [
                {
                    "name": "insert-enriched-orders",
                    "spec": {
                        "statement": (
                            "INSERT INTO enriched_orders "
                            "SELECT o.order_id, c.name "
                            "FROM orders o JOIN customers c ON o.customer_id = c.id"
                        ),
                        "compute_pool": {"id": "lfcp-abc123"},
                        "principal": "sa-lineage-bridge",
                    },
                    "status": {"phase": "RUNNING"},
                }
            ],
            "metadata": {"next": None},
        }
        respx.get(f"{FLINK_URL}{stmt_path}").mock(
            return_value=httpx.Response(200, json=statements_resp)
        )

        nodes, edges = await flink_client.extract()

        # Expect: 1 flink_job + 2 source topics + 1 sink topic = 4 nodes
        assert len(nodes) == 4

        job_nodes = [n for n in nodes if n.node_type == NodeType.FLINK_JOB]
        assert len(job_nodes) == 1
        assert job_nodes[0].display_name == "insert-enriched-orders"
        assert job_nodes[0].system == SystemType.CONFLUENT

        topic_nodes = [n for n in nodes if n.node_type == NodeType.KAFKA_TOPIC]
        topic_names = {n.display_name for n in topic_nodes}
        assert topic_names == {"orders", "customers", "enriched_orders"}

        # Expect: 2 CONSUMES (source -> job) + 1 PRODUCES (job -> sink) = 3 edges
        assert len(edges) == 3

        consumes = [e for e in edges if e.edge_type == EdgeType.CONSUMES]
        produces = [e for e in edges if e.edge_type == EdgeType.PRODUCES]
        assert len(consumes) == 2
        assert len(produces) == 1

        # CONSUMES edges should have confidence < 1.0
        for e in consumes:
            assert e.confidence == 0.8


# ── helpers ───────────────────────────────────────────────────────────────


async def _no_sleep(seconds: float) -> None:
    pass
