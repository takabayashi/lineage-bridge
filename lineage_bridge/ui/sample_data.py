"""Generate a realistic sample LineageGraph for UI testing."""

from __future__ import annotations

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

ENV_ID = "env-abc123"
CLUSTER_ID = "lkc-xyz789"
SR_CLUSTER_ID = "lsrc-sr001"
CC_BASE_URL = "https://confluent.cloud/environments"


def _node_id(system: str, ntype: str, name: str) -> str:
    return f"{system}:{ntype}:{ENV_ID}:{name}"


def generate_sample_graph() -> LineageGraph:
    """Build a realistic demo graph with connectors, topics, processing, and sinks."""
    graph = LineageGraph()

    # ── External datasets ──────────────────────────────────────────────
    ext_pg = LineageNode(
        node_id=_node_id("external", "external_dataset", "postgres.public.orders"),
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="postgres.public.orders",
        display_name="PostgreSQL orders",
        environment_id=ENV_ID,
        attributes={"database": "postgres", "schema": "public", "table": "orders"},
        tags=["source", "cdc"],
        url=None,
    )
    ext_s3 = LineageNode(
        node_id=_node_id("external", "external_dataset", "s3://data-lake/orders"),
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="s3://data-lake/orders",
        display_name="S3 data-lake/orders",
        environment_id=ENV_ID,
        attributes={"bucket": "data-lake", "prefix": "orders/"},
        tags=["sink", "data-lake"],
    )
    graph.add_node(ext_pg)
    graph.add_node(ext_s3)

    # ── Source connectors ──────────────────────────────────────────────
    conn_pg = LineageNode(
        node_id=_node_id("confluent", "connector", "pg-cdc-source"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="pg-cdc-source",
        display_name="PostgreSQL CDC Source",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"connector.class": "PostgresCdcSource", "status": "RUNNING"},
        tags=["source", "cdc"],
        url=f"{CC_BASE_URL}/{ENV_ID}/connectors/pg-cdc-source",
    )
    conn_mysql = LineageNode(
        node_id=_node_id("confluent", "connector", "mysql-cdc-source"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="mysql-cdc-source",
        display_name="MySQL CDC Source",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"connector.class": "MySqlCdcSource", "status": "RUNNING"},
        tags=["source", "cdc"],
        url=f"{CC_BASE_URL}/{ENV_ID}/connectors/mysql-cdc-source",
    )
    graph.add_node(conn_pg)
    graph.add_node(conn_mysql)

    # ── Kafka topics ───────────────────────────────────────────────────
    topic_names = ["orders", "customers", "enriched_orders", "orders_avro", "shipments"]
    topics: dict[str, LineageNode] = {}
    for t in topic_names:
        node = LineageNode(
            node_id=_node_id("confluent", "kafka_topic", t),
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name=t,
            display_name=t,
            environment_id=ENV_ID,
            cluster_id=CLUSTER_ID,
            attributes={"partitions": 6, "replication_factor": 3, "retention.ms": 604800000},
            tags=["kafka"],
            url=f"{CC_BASE_URL}/{ENV_ID}/clusters/{CLUSTER_ID}/topics/{t}",
        )
        topics[t] = node
        graph.add_node(node)

    # ── Schemas ────────────────────────────────────────────────────────
    schema_orders = LineageNode(
        node_id=_node_id("confluent", "schema", "orders-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="orders-value",
        display_name="orders-value (Avro v3)",
        environment_id=ENV_ID,
        cluster_id=SR_CLUSTER_ID,
        attributes={"format": "AVRO", "version": 3, "compatibility": "BACKWARD"},
        tags=["schema"],
        url=f"{CC_BASE_URL}/{ENV_ID}/schema-registry/schemas/orders-value",
    )
    schema_customers = LineageNode(
        node_id=_node_id("confluent", "schema", "customers-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="customers-value",
        display_name="customers-value (Avro v1)",
        environment_id=ENV_ID,
        cluster_id=SR_CLUSTER_ID,
        attributes={"format": "AVRO", "version": 1, "compatibility": "BACKWARD"},
        tags=["schema"],
        url=f"{CC_BASE_URL}/{ENV_ID}/schema-registry/schemas/customers-value",
    )
    graph.add_node(schema_orders)
    graph.add_node(schema_customers)

    # ── ksqlDB query ───────────────────────────────────────────────────
    ksql = LineageNode(
        node_id=_node_id("confluent", "ksqldb_query", "enrich_orders_stream"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.KSQLDB_QUERY,
        qualified_name="enrich_orders_stream",
        display_name="Enrich Orders (ksqlDB)",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={
            "sql": (
                "CREATE STREAM enriched_orders AS "
                "SELECT o.*, c.name, c.email "
                "FROM orders o JOIN customers c ON o.customer_id = c.id;"
            ),
            "status": "RUNNING",
        },
        tags=["ksqldb", "streaming"],
        url=f"{CC_BASE_URL}/{ENV_ID}/ksqldb/queries/enrich_orders_stream",
    )
    graph.add_node(ksql)

    # ── Flink job ──────────────────────────────────────────────────────
    flink = LineageNode(
        node_id=_node_id("confluent", "flink_job", "orders-avro-transform"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.FLINK_JOB,
        qualified_name="orders-avro-transform",
        display_name="Orders Avro Transform (Flink)",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"parallelism": 4, "status": "RUNNING"},
        tags=["flink", "streaming"],
        url=f"{CC_BASE_URL}/{ENV_ID}/flink/jobs/orders-avro-transform",
    )
    graph.add_node(flink)

    # ── Sink connectors ────────────────────────────────────────────────
    conn_s3 = LineageNode(
        node_id=_node_id("confluent", "connector", "s3-sink"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="s3-sink",
        display_name="S3 Sink Connector",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"connector.class": "S3SinkConnector", "status": "RUNNING"},
        tags=["sink", "s3"],
        url=f"{CC_BASE_URL}/{ENV_ID}/connectors/s3-sink",
    )
    conn_es = LineageNode(
        node_id=_node_id("confluent", "connector", "elasticsearch-sink"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="elasticsearch-sink",
        display_name="Elasticsearch Sink",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"connector.class": "ElasticsearchSinkConnector", "status": "RUNNING"},
        tags=["sink", "search"],
        url=f"{CC_BASE_URL}/{ENV_ID}/connectors/elasticsearch-sink",
    )
    graph.add_node(conn_s3)
    graph.add_node(conn_es)

    # ── Tableflow table ────────────────────────────────────────────────
    tf_table = LineageNode(
        node_id=_node_id("confluent", "tableflow_table", "orders_avro"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.TABLEFLOW_TABLE,
        qualified_name="orders_avro",
        display_name="orders_avro (Tableflow)",
        environment_id=ENV_ID,
        cluster_id=CLUSTER_ID,
        attributes={"storage_format": "iceberg", "status": "ACTIVE"},
        tags=["tableflow", "iceberg"],
        url=f"{CC_BASE_URL}/{ENV_ID}/tableflow/orders_avro",
    )
    graph.add_node(tf_table)

    # ── Unity Catalog table ────────────────────────────────────────────
    uc_table = LineageNode(
        node_id=_node_id("databricks", "uc_table", "catalog.lkc-xyz789.orders_avro"),
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="catalog.lkc-xyz789.orders_avro",
        display_name="UC: catalog.orders_avro",
        environment_id=ENV_ID,
        attributes={"catalog": "catalog", "schema": "lkc-xyz789", "table": "orders_avro"},
        tags=["unity-catalog", "databricks"],
    )
    graph.add_node(uc_table)

    # ── Consumer groups ────────────────────────────────────────────────
    cg_names = ["orders-processing-cg", "analytics-cg", "shipment-tracker-cg"]
    cg_nodes: dict[str, LineageNode] = {}
    for cg in cg_names:
        node = LineageNode(
            node_id=_node_id("confluent", "consumer_group", cg),
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONSUMER_GROUP,
            qualified_name=cg,
            display_name=cg,
            environment_id=ENV_ID,
            cluster_id=CLUSTER_ID,
            attributes={"state": "Stable", "members": 3},
            tags=["consumer-group"],
        )
        cg_nodes[cg] = node
        graph.add_node(node)

    # ── Edges ──────────────────────────────────────────────────────────

    def _edge(src: str, dst: str, etype: EdgeType) -> LineageEdge:
        return LineageEdge(src_id=src, dst_id=dst, edge_type=etype)

    # External PG -> PG CDC connector -> orders topic
    graph.add_edge(_edge(ext_pg.node_id, conn_pg.node_id, EdgeType.PRODUCES))
    graph.add_edge(_edge(conn_pg.node_id, topics["orders"].node_id, EdgeType.PRODUCES))

    # MySQL CDC connector -> customers topic
    graph.add_edge(_edge(conn_mysql.node_id, topics["customers"].node_id, EdgeType.PRODUCES))

    # Schemas
    graph.add_edge(_edge(topics["orders"].node_id, schema_orders.node_id, EdgeType.HAS_SCHEMA))
    graph.add_edge(
        _edge(topics["customers"].node_id, schema_customers.node_id, EdgeType.HAS_SCHEMA)
    )

    # ksqlDB: orders + customers -> enriched_orders
    graph.add_edge(_edge(topics["orders"].node_id, ksql.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(topics["customers"].node_id, ksql.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(ksql.node_id, topics["enriched_orders"].node_id, EdgeType.PRODUCES))

    # Flink: orders -> orders_avro
    graph.add_edge(_edge(topics["orders"].node_id, flink.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(flink.node_id, topics["orders_avro"].node_id, EdgeType.PRODUCES))

    # PG CDC connector -> shipments
    graph.add_edge(_edge(conn_pg.node_id, topics["shipments"].node_id, EdgeType.PRODUCES))

    # Sink: enriched_orders -> Elasticsearch sink
    graph.add_edge(_edge(topics["enriched_orders"].node_id, conn_es.node_id, EdgeType.CONSUMES))

    # Sink: orders_avro -> S3 sink -> S3 bucket
    graph.add_edge(_edge(topics["orders_avro"].node_id, conn_s3.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(conn_s3.node_id, ext_s3.node_id, EdgeType.PRODUCES))

    # Tableflow: orders_avro topic -> tableflow table
    graph.add_edge(_edge(topics["orders_avro"].node_id, tf_table.node_id, EdgeType.MATERIALIZES))

    # UC table: tableflow -> UC
    graph.add_edge(_edge(tf_table.node_id, uc_table.node_id, EdgeType.MATERIALIZES))

    # Consumer groups
    graph.add_edge(
        _edge(topics["orders"].node_id, cg_nodes["orders-processing-cg"].node_id, EdgeType.CONSUMES)
    )
    graph.add_edge(
        _edge(
            topics["enriched_orders"].node_id,
            cg_nodes["analytics-cg"].node_id,
            EdgeType.CONSUMES,
        )
    )
    graph.add_edge(
        _edge(
            topics["shipments"].node_id,
            cg_nodes["shipment-tracker-cg"].node_id,
            EdgeType.CONSUMES,
        )
    )

    return graph
