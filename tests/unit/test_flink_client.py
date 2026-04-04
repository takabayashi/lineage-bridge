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

    def test_ctas_with_with_clause(self):
        """CREATE TABLE ... WITH (...) AS SELECT should detect sink."""
        sql = (
            "CREATE TABLE best_stocks\n"
            "WITH ('connector' = 'confluent') AS\n"
            "SELECT s.symbol FROM stocks s"
        )
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"best_stocks"}
        assert sources == {"stocks"}

    def test_tumble_window_source(self):
        """TUMBLE(TABLE <name>, ...) should extract the table as a source."""
        sql = (
            "CREATE TABLE agg_output WITH ('connector' = 'confluent') AS\n"
            "SELECT window_start, s.symbol, SUM(s.qty) AS total\n"
            "FROM TUMBLE (\n"
            "  TABLE stocks,\n"
            "  DESCRIPTOR($rowtime),\n"
            "  INTERVAL '10' MINUTES\n"
            ") AS s\n"
            "GROUP BY window_start, s.symbol"
        )
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"agg_output"}
        assert "stocks" in sources
        assert "TUMBLE" not in sources

    def test_hop_window_source(self):
        """HOP(TABLE <name>, ...) should extract the table as a source."""
        sql = (
            "INSERT INTO output_table\n"
            "SELECT * FROM HOP(TABLE input_events, "
            "DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)"
        )
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"output_table"}
        assert "input_events" in sources

    def test_set_prefix_stripped(self):
        """SET commands before the real SQL should not break parsing."""
        sql = (
            "SET 'client.statement-name' 'MY JOB';\n"
            "CREATE TABLE IF NOT EXISTS best_stocks\n"
            "WITH ('connector' = 'confluent') AS\n"
            "SELECT s.symbol FROM TUMBLE(TABLE stocks, "
            "DESCRIPTOR($rowtime), INTERVAL '10' MINUTES) AS s\n"
            "GROUP BY s.symbol"
        )
        # The SET stripping happens in extract(), not _parse_flink_sql,
        # so we strip it here manually for the parser test.
        import re
        sql = re.sub(
            r"^(\s*SET\s+'[^']*'\s+'[^']*'\s*;\s*)+",
            "", sql, flags=re.IGNORECASE,
        ).strip()
        sources, sinks = FlinkClient._parse_flink_sql(sql)
        assert sinks == {"best_stocks"}
        assert "stocks" in sources


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

    @respx.mock
    async def test_extract_stopped_ctas_with_tumble(self, flink_client, monkeypatch):
        """STOPPED CTAS with TUMBLE windowing should produce lineage."""
        monkeypatch.setattr("asyncio.sleep", _no_sleep)

        pools_resp = {
            "data": [{"id": "lfcp-1", "spec": {"region": "us-east-1", "cloud": "AWS"}}],
            "metadata": {"next": None},
        }
        respx.get(f"{CLOUD_URL}/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(200, json=pools_resp)
        )

        stmt_path = f"/sql/v1/organizations/{ORG_ID}/environments/{ENV_ID}/statements"
        statements_resp = {
            "data": [
                {
                    "name": "ctas-best-stocks",
                    "spec": {
                        "statement": (
                            "CREATE TABLE best_stocks\n"
                            "WITH ('connector' = 'confluent') AS\n"
                            "SELECT s.symbol, SUM(s.qty) AS total\n"
                            "FROM TUMBLE(TABLE stocks, "
                            "DESCRIPTOR($rowtime), INTERVAL '10' MINUTES) AS s\n"
                            "GROUP BY s.symbol"
                        ),
                        "compute_pool": {"id": "lfcp-1"},
                    },
                    "status": {"phase": "STOPPED"},
                }
            ],
            "metadata": {"next": None},
        }
        respx.get(f"{FLINK_URL}{stmt_path}").mock(
            return_value=httpx.Response(200, json=statements_resp)
        )

        nodes, edges = await flink_client.extract()

        job_nodes = [n for n in nodes if n.node_type == NodeType.FLINK_JOB]
        assert len(job_nodes) == 1
        assert job_nodes[0].attributes["phase"] == "STOPPED"

        topic_names = {n.display_name for n in nodes if n.node_type == NodeType.KAFKA_TOPIC}
        assert "stocks" in topic_names
        assert "best_stocks" in topic_names

        consumes = [e for e in edges if e.edge_type == EdgeType.CONSUMES]
        produces = [e for e in edges if e.edge_type == EdgeType.PRODUCES]
        assert len(consumes) == 1
        assert len(produces) == 1


# ── helpers ───────────────────────────────────────────────────────────────


async def _no_sleep(seconds: float) -> None:
    pass
