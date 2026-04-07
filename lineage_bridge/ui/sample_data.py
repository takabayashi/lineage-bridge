# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
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
ENV_NAME = "Production"
CLUSTER_ID = "lkc-xyz789"
CLUSTER_NAME = "orders-cluster"
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
        environment_name=ENV_NAME,
        attributes={
            "database": "postgres",
            "schema": "public",
            "table": "orders",
            "inferred_from": "pg-cdc-source",
        },
        tags=["source", "cdc"],
    )
    ext_s3 = LineageNode(
        node_id=_node_id("external", "external_dataset", "s3://data-lake/orders"),
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="s3://data-lake/orders",
        display_name="S3 data-lake/orders",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "bucket": "data-lake",
            "prefix": "orders/",
            "inferred_from": "s3-sink",
        },
        tags=["sink", "data-lake"],
    )
    ext_es = LineageNode(
        node_id=_node_id("external", "external_dataset", "elasticsearch.enriched-orders"),
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="elasticsearch.enriched-orders",
        display_name="Elasticsearch enriched-orders",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "index": "enriched-orders",
            "inferred_from": "elasticsearch-sink",
        },
        tags=["sink", "search"],
    )
    graph.add_node(ext_pg)
    graph.add_node(ext_s3)
    graph.add_node(ext_es)

    # ── Source connectors ──────────────────────────────────────────────
    conn_pg = LineageNode(
        node_id=_node_id("confluent", "connector", "pg-cdc-source"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="pg-cdc-source",
        display_name="PostgreSQL CDC Source",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "connector_class": "io.confluent.connect.jdbc.PostgresCdcSourceConnector",
            "direction": "source",
            "tasks_max": 3,
            "output_data_format": "AVRO",
            "state": "RUNNING",
        },
        tags=["source", "cdc"],
        url=f"{CC_BASE_URL}/{ENV_ID}/clusters/{CLUSTER_ID}/connectors/pg-cdc-source/overview",
    )
    conn_mysql = LineageNode(
        node_id=_node_id("confluent", "connector", "mysql-cdc-source"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="mysql-cdc-source",
        display_name="MySQL CDC Source",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "connector_class": "io.debezium.connector.mysql.MySqlConnector",
            "direction": "source",
            "tasks_max": 2,
            "output_data_format": "JSON_SR",
            "state": "RUNNING",
        },
        tags=["source", "cdc"],
        url=f"{CC_BASE_URL}/{ENV_ID}/clusters/{CLUSTER_ID}/connectors/mysql-cdc-source/overview",
    )
    graph.add_node(conn_pg)
    graph.add_node(conn_mysql)

    # ── Kafka topics ───────────────────────────────────────────────────
    topic_configs = {
        "orders": {
            "partitions_count": 12,
            "replication_factor": 3,
            "description": "Raw order events from PostgreSQL CDC",
            "owner": "data-platform-team",
            "metrics_active": True,
            "metrics_received_records": 184_520,
            "metrics_sent_records": 184_520,
            "metrics_received_bytes": 42_800_000,
            "metrics_sent_bytes": 42_800_000,
            "metrics_window_hours": 1,
        },
        "customers": {
            "partitions_count": 6,
            "replication_factor": 3,
            "description": "Customer master data from MySQL CDC",
            "owner": "data-platform-team",
            "metrics_active": True,
            "metrics_received_records": 12_340,
            "metrics_sent_records": 12_340,
            "metrics_received_bytes": 3_200_000,
            "metrics_sent_bytes": 3_200_000,
            "metrics_window_hours": 1,
        },
        "enriched_orders": {
            "partitions_count": 12,
            "replication_factor": 3,
            "description": "Orders enriched with customer info via ksqlDB join",
            "owner": "streaming-analytics",
            "metrics_active": True,
            "metrics_received_records": 184_520,
            "metrics_sent_records": 184_520,
            "metrics_received_bytes": 58_900_000,
            "metrics_sent_bytes": 58_900_000,
            "metrics_window_hours": 1,
        },
        "orders_avro": {
            "partitions_count": 12,
            "replication_factor": 3,
            "description": "Orders converted to Avro format via Flink",
            "owner": "data-platform-team",
            "metrics_active": True,
            "metrics_received_records": 184_520,
            "metrics_sent_records": 184_520,
            "metrics_received_bytes": 38_100_000,
            "metrics_sent_bytes": 38_100_000,
            "metrics_window_hours": 1,
        },
        "shipments": {
            "partitions_count": 6,
            "replication_factor": 3,
            "description": "Shipment tracking events from PostgreSQL",
            "owner": "logistics-team",
            "metrics_active": True,
            "metrics_received_records": 45_210,
            "metrics_sent_records": 45_210,
            "metrics_received_bytes": 11_400_000,
            "metrics_sent_bytes": 11_400_000,
            "metrics_window_hours": 1,
        },
        "payments": {
            "partitions_count": 8,
            "replication_factor": 3,
            "description": "Payment transaction events",
            "owner": "billing-team",
            "metrics_active": True,
            "metrics_received_records": 67_890,
            "metrics_sent_records": 67_890,
            "metrics_received_bytes": 18_200_000,
            "metrics_sent_bytes": 18_200_000,
            "metrics_window_hours": 1,
        },
        "order_status_updates": {
            "partitions_count": 6,
            "replication_factor": 3,
            "is_internal": False,
            "description": "Real-time order status changes aggregated by Flink",
            "owner": "streaming-analytics",
            "metrics_active": True,
            "metrics_received_records": 92_150,
            "metrics_sent_records": 92_150,
            "metrics_received_bytes": 14_800_000,
            "metrics_sent_bytes": 14_800_000,
            "metrics_window_hours": 1,
        },
    }
    topics: dict[str, LineageNode] = {}
    for t, attrs in topic_configs.items():
        node = LineageNode(
            node_id=_node_id("confluent", "kafka_topic", t),
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name=t,
            display_name=t,
            environment_id=ENV_ID,
            environment_name=ENV_NAME,
            cluster_id=CLUSTER_ID,
            cluster_name=CLUSTER_NAME,
            attributes=attrs,
            tags=["kafka"],
            url=f"{CC_BASE_URL}/{ENV_ID}/clusters/{CLUSTER_ID}/topics/{t}/overview",
        )
        topics[t] = node
        graph.add_node(node)

    # ── Schemas ────────────────────────────────────────────────────────
    schema_orders_key = LineageNode(
        node_id=_node_id("confluent", "schema", "orders-key"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="orders-key",
        display_name="orders-key",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=SR_CLUSTER_ID,
        attributes={
            "schema_type": "AVRO",
            "version": 1,
            "field_count": 1,
            "schema_id": 100001,
            "schema_string": (
                '{"type":"record","name":"OrderKey","namespace":"com.example",'
                '"fields":[{"name":"order_id","type":"string"}]}'
            ),
        },
        tags=["schema", "avro"],
    )
    schema_orders_value = LineageNode(
        node_id=_node_id("confluent", "schema", "orders-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="orders-value",
        display_name="orders-value",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=SR_CLUSTER_ID,
        attributes={
            "schema_type": "AVRO",
            "version": 3,
            "field_count": 8,
            "schema_id": 100042,
            "schema_string": (
                '{"type":"record","name":"Order","namespace":"com.example",'
                '"fields":[{"name":"order_id","type":"string"},'
                '{"name":"customer_id","type":"string"},'
                '{"name":"product","type":"string"},'
                '{"name":"quantity","type":"int"},'
                '{"name":"price","type":"double"},'
                '{"name":"currency","type":"string"},'
                '{"name":"status","type":"string"},'
                '{"name":"created_at","type":"long"}]}'
            ),
        },
        tags=["schema", "avro"],
    )
    schema_customers = LineageNode(
        node_id=_node_id("confluent", "schema", "customers-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="customers-value",
        display_name="customers-value",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=SR_CLUSTER_ID,
        attributes={
            "schema_type": "AVRO",
            "version": 1,
            "field_count": 5,
            "schema_id": 100018,
            "schema_string": (
                '{"type":"record","name":"Customer","namespace":"com.example",'
                '"fields":[{"name":"id","type":"string"},'
                '{"name":"name","type":"string"},'
                '{"name":"email","type":"string"},'
                '{"name":"tier","type":"string"},'
                '{"name":"created_at","type":"long"}]}'
            ),
        },
        tags=["schema", "avro"],
    )
    schema_enriched = LineageNode(
        node_id=_node_id("confluent", "schema", "enriched_orders-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="enriched_orders-value",
        display_name="enriched_orders-value",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=SR_CLUSTER_ID,
        attributes={
            "schema_type": "AVRO",
            "version": 2,
            "field_count": 7,
            "schema_id": 100055,
            "schema_string": (
                '{"type":"record","name":"EnrichedOrder","namespace":"com.example",'
                '"fields":[{"name":"order_id","type":"string"},'
                '{"name":"product","type":"string"},'
                '{"name":"quantity","type":"int"},'
                '{"name":"price","type":"double"},'
                '{"name":"customer_name","type":"string"},'
                '{"name":"customer_email","type":"string"},'
                '{"name":"customer_tier","type":"string"}]}'
            ),
        },
        tags=["schema", "avro"],
    )
    schema_payments = LineageNode(
        node_id=_node_id("confluent", "schema", "payments-value"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="payments-value",
        display_name="payments-value",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=SR_CLUSTER_ID,
        attributes={
            "schema_type": "JSON",
            "version": 1,
            "field_count": 6,
            "schema_id": 100070,
            "schema_string": (
                '{"type":"object","properties":{'
                '"payment_id":{"type":"string"},'
                '"order_id":{"type":"string"},'
                '"amount":{"type":"number"},'
                '"currency":{"type":"string"},'
                '"method":{"type":"string"},'
                '"paid_at":{"type":"string","format":"date-time"}}}'
            ),
        },
        tags=["schema", "json"],
    )
    graph.add_node(schema_orders_key)
    graph.add_node(schema_orders_value)
    graph.add_node(schema_customers)
    graph.add_node(schema_enriched)
    graph.add_node(schema_payments)

    # ── ksqlDB queries ─────────────────────────────────────────────────
    ksql_enrich = LineageNode(
        node_id=_node_id("confluent", "ksqldb_query", "CSAS_ENRICHED_ORDERS_0"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.KSQLDB_QUERY,
        qualified_name="CSAS_ENRICHED_ORDERS_0",
        display_name="Enrich Orders (ksqlDB)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "sql": (
                "CREATE STREAM enriched_orders AS\n"
                "SELECT o.order_id, o.product, o.quantity, o.price,\n"
                "       c.name AS customer_name, c.email, c.tier\n"
                "FROM orders o\n"
                "  JOIN customers c\n"
                "    ON o.customer_id = c.id\n"
                "EMIT CHANGES;"
            ),
            "state": "RUNNING",
            "ksqldb_cluster_id": "lksqlc-demo01",
        },
        tags=["ksqldb", "streaming"],
    )
    ksql_status = LineageNode(
        node_id=_node_id("confluent", "ksqldb_query", "CTAS_ORDER_STATUS_0"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.KSQLDB_QUERY,
        qualified_name="CTAS_ORDER_STATUS_0",
        display_name="Order Status Table (ksqlDB)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "sql": (
                "CREATE TABLE order_status AS\n"
                "SELECT order_id,\n"
                "       LATEST_BY_OFFSET(status) AS current_status,\n"
                "       COUNT(*) AS update_count\n"
                "FROM orders\n"
                "GROUP BY order_id\n"
                "EMIT CHANGES;"
            ),
            "state": "RUNNING",
            "ksqldb_cluster_id": "lksqlc-demo01",
        },
        tags=["ksqldb", "streaming"],
    )
    graph.add_node(ksql_enrich)
    graph.add_node(ksql_status)

    # ── Flink jobs ─────────────────────────────────────────────────────
    flink_avro = LineageNode(
        node_id=_node_id("confluent", "flink_job", "orders-avro-transform"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.FLINK_JOB,
        qualified_name="orders-avro-transform",
        display_name="Orders Avro Transform (Flink)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "phase": "RUNNING",
            "compute_pool_id": "lfcp-pool01",
            "principal": "sa-demo-flink",
            "sql": (
                "INSERT INTO orders_avro\n"
                "SELECT order_id, customer_id, product,\n"
                "       quantity, price, currency,\n"
                "       status, created_at\n"
                "FROM orders;"
            ),
        },
        tags=["flink", "streaming"],
    )
    flink_status = LineageNode(
        node_id=_node_id("confluent", "flink_job", "order-status-aggregator"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.FLINK_JOB,
        qualified_name="order-status-aggregator",
        display_name="Order Status Aggregator (Flink)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "phase": "RUNNING",
            "compute_pool_id": "lfcp-pool01",
            "principal": "sa-demo-flink",
            "sql": (
                "INSERT INTO order_status_updates\n"
                "SELECT order_id, status, updated_at,\n"
                "       COUNT(*) OVER (PARTITION BY order_id) AS status_count\n"
                "FROM orders\n"
                "WHERE status IS NOT NULL;"
            ),
        },
        tags=["flink", "streaming"],
    )
    graph.add_node(flink_avro)
    graph.add_node(flink_status)

    # ── Sink connectors ────────────────────────────────────────────────
    conn_s3 = LineageNode(
        node_id=_node_id("confluent", "connector", "s3-sink"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="s3-sink",
        display_name="S3 Sink Connector",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "connector_class": "io.confluent.connect.s3.S3SinkConnector",
            "direction": "sink",
            "tasks_max": 4,
            "output_data_format": "PARQUET",
            "state": "RUNNING",
        },
        tags=["sink", "s3"],
    )
    conn_es = LineageNode(
        node_id=_node_id("confluent", "connector", "elasticsearch-sink"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name="elasticsearch-sink",
        display_name="Elasticsearch Sink",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "connector_class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
            "direction": "sink",
            "tasks_max": 2,
            "output_data_format": "JSON",
            "state": "RUNNING",
        },
        tags=["sink", "search"],
    )
    graph.add_node(conn_s3)
    graph.add_node(conn_es)

    # ── Tableflow tables ───────────────────────────────────────────────
    tf_orders = LineageNode(
        node_id=_node_id("confluent", "tableflow_table", f"{CLUSTER_ID}.orders_avro"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.TABLEFLOW_TABLE,
        qualified_name=f"{CLUSTER_ID}.orders_avro",
        display_name="orders_avro (Tableflow)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "phase": "ACTIVE",
            "table_formats": ["ICEBERG"],
            "storage_kind": "ConfluentManaged",
            "table_path": "s3://confluent-tableflow/orders_avro",
        },
        tags=["tableflow", "iceberg"],
    )
    tf_payments = LineageNode(
        node_id=_node_id("confluent", "tableflow_table", f"{CLUSTER_ID}.payments"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.TABLEFLOW_TABLE,
        qualified_name=f"{CLUSTER_ID}.payments",
        display_name="payments (Tableflow)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "phase": "ACTIVE",
            "table_formats": ["ICEBERG"],
            "storage_kind": "ConfluentManaged",
            "table_path": "s3://confluent-tableflow/payments",
        },
        tags=["tableflow", "iceberg"],
    )
    tf_enriched = LineageNode(
        node_id=_node_id("confluent", "tableflow_table", f"{CLUSTER_ID}.enriched_orders"),
        system=SystemType.CONFLUENT,
        node_type=NodeType.TABLEFLOW_TABLE,
        qualified_name=f"{CLUSTER_ID}.enriched_orders",
        display_name="enriched_orders (Tableflow)",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        cluster_id=CLUSTER_ID,
        cluster_name=CLUSTER_NAME,
        attributes={
            "phase": "SUSPENDED",
            "suspended": True,
            "table_formats": ["ICEBERG", "DELTA"],
            "storage_kind": "ByobS3",
            "table_path": "s3://customer-lake/enriched_orders",
        },
        tags=["tableflow", "iceberg", "delta"],
    )
    graph.add_node(tf_orders)
    graph.add_node(tf_payments)
    graph.add_node(tf_enriched)

    # ── Unity Catalog tables ──────────────────────────────────────────
    uc_orders = LineageNode(
        node_id=_node_id("databricks", "uc_table", "main.streaming.orders_avro"),
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="main.streaming.orders_avro",
        display_name="UC: main.streaming.orders_avro",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "catalog_name": "main",
            "schema_name": "streaming",
            "table_name": "orders_avro",
            "catalog_type": "MANAGED",
            "table_type": "EXTERNAL",
            "data_source_format": "ICEBERG",
            "workspace_url": "https://dbc-demo.cloud.databricks.com",
            "storage_location": "s3://confluent-tableflow/orders_avro",
            "columns": [
                {"name": "order_id", "type": "STRING", "position": 0},
                {"name": "customer_id", "type": "STRING", "position": 1},
                {"name": "product", "type": "STRING", "position": 2},
                {"name": "quantity", "type": "INT", "position": 3},
                {"name": "price", "type": "DOUBLE", "position": 4},
                {"name": "currency", "type": "STRING", "position": 5},
                {"name": "status", "type": "STRING", "position": 6},
                {"name": "created_at", "type": "BIGINT", "position": 7},
            ],
        },
        tags=["unity-catalog", "databricks"],
    )
    uc_enriched = LineageNode(
        node_id=_node_id("databricks", "uc_table", "main.streaming.enriched_orders"),
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="main.streaming.enriched_orders",
        display_name="UC: main.streaming.enriched_orders",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "catalog_name": "main",
            "schema_name": "streaming",
            "table_name": "enriched_orders",
            "catalog_type": "MANAGED",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "workspace_url": "https://dbc-demo.cloud.databricks.com",
            "storage_location": "s3://customer-lake/enriched_orders",
            "columns": [
                {"name": "order_id", "type": "STRING", "position": 0},
                {"name": "product", "type": "STRING", "position": 1},
                {"name": "quantity", "type": "INT", "position": 2},
                {"name": "price", "type": "DOUBLE", "position": 3},
                {"name": "customer_name", "type": "STRING", "position": 4},
                {"name": "customer_email", "type": "STRING", "position": 5},
                {"name": "customer_tier", "type": "STRING", "position": 6},
            ],
        },
        tags=["unity-catalog", "databricks"],
    )
    # UC lineage-discovered downstream table (TRANSFORMS edge)
    uc_daily_summary = LineageNode(
        node_id=_node_id("databricks", "uc_table", "main.analytics.daily_order_summary"),
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="main.analytics.daily_order_summary",
        display_name="UC: daily_order_summary",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_name": "daily_order_summary",
            "catalog_type": "MANAGED",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "workspace_url": "https://dbc-demo.cloud.databricks.com",
            "columns": [
                {"name": "order_date", "type": "DATE", "position": 0},
                {"name": "total_orders", "type": "BIGINT", "position": 1},
                {"name": "total_revenue", "type": "DOUBLE", "position": 2},
                {"name": "avg_order_value", "type": "DOUBLE", "position": 3},
            ],
        },
        tags=["unity-catalog", "databricks", "analytics"],
    )
    graph.add_node(uc_orders)
    graph.add_node(uc_enriched)
    graph.add_node(uc_daily_summary)

    # ── AWS Glue tables ────────────────────────────────────────────────
    glue_payments = LineageNode(
        node_id=_node_id("aws", "glue_table", "streaming_db.payments"),
        system=SystemType.AWS,
        node_type=NodeType.GLUE_TABLE,
        qualified_name="streaming_db.payments",
        display_name="Glue: streaming_db.payments",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "database": "streaming_db",
            "table_name": "payments",
            "aws_region": "us-east-1",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://confluent-tableflow/payments",
            "serde_info": "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
            "columns": [
                {"name": "payment_id", "type": "string"},
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "currency", "type": "string"},
                {"name": "method", "type": "string"},
                {"name": "paid_at", "type": "timestamp"},
            ],
        },
        tags=["aws-glue"],
    )
    glue_shipments = LineageNode(
        node_id=_node_id("aws", "glue_table", "streaming_db.shipments"),
        system=SystemType.AWS,
        node_type=NodeType.GLUE_TABLE,
        qualified_name="streaming_db.shipments",
        display_name="Glue: streaming_db.shipments",
        environment_id=ENV_ID,
        environment_name=ENV_NAME,
        attributes={
            "database": "streaming_db",
            "table_name": "shipments",
            "aws_region": "us-east-1",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://confluent-tableflow/shipments",
            "columns": [
                {"name": "shipment_id", "type": "string"},
                {"name": "order_id", "type": "string"},
                {"name": "carrier", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "shipped_at", "type": "timestamp"},
            ],
        },
        tags=["aws-glue"],
    )
    graph.add_node(glue_payments)
    graph.add_node(glue_shipments)

    # ── Consumer groups ────────────────────────────────────────────────
    cg_configs = {
        "orders-processing-cg": {"state": "Stable", "is_simple": False},
        "analytics-cg": {"state": "Stable", "is_simple": False},
        "shipment-tracker-cg": {"state": "Stable", "is_simple": True},
        "flink-orders-consumer": {"state": "REBALANCING", "is_simple": False},
    }
    cg_nodes: dict[str, LineageNode] = {}
    for cg, attrs in cg_configs.items():
        node = LineageNode(
            node_id=_node_id("confluent", "consumer_group", cg),
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONSUMER_GROUP,
            qualified_name=cg,
            display_name=cg,
            environment_id=ENV_ID,
            environment_name=ENV_NAME,
            cluster_id=CLUSTER_ID,
            cluster_name=CLUSTER_NAME,
            attributes=attrs,
            tags=["consumer-group"],
        )
        cg_nodes[cg] = node
        graph.add_node(node)

    # ══════════════════════════════════════════════════════════════════
    #  EDGES
    # ══════════════════════════════════════════════════════════════════

    def _edge(src: str, dst: str, etype: EdgeType, **attrs) -> LineageEdge:
        return LineageEdge(src_id=src, dst_id=dst, edge_type=etype, attributes=attrs)

    # ── External → Connectors → Topics (PRODUCES) ─────────────────────
    graph.add_edge(_edge(ext_pg.node_id, conn_pg.node_id, EdgeType.PRODUCES))
    graph.add_edge(_edge(conn_pg.node_id, topics["orders"].node_id, EdgeType.PRODUCES))
    graph.add_edge(_edge(conn_pg.node_id, topics["shipments"].node_id, EdgeType.PRODUCES))
    graph.add_edge(_edge(conn_mysql.node_id, topics["customers"].node_id, EdgeType.PRODUCES))

    # ── Schemas (key + value roles) ───────────────────────────────────
    graph.add_edge(
        _edge(topics["orders"].node_id, schema_orders_key.node_id, EdgeType.HAS_SCHEMA, role="key")
    )
    graph.add_edge(
        _edge(
            topics["orders"].node_id,
            schema_orders_value.node_id,
            EdgeType.HAS_SCHEMA,
            role="value",
        )
    )
    graph.add_edge(
        _edge(
            topics["customers"].node_id,
            schema_customers.node_id,
            EdgeType.HAS_SCHEMA,
            role="value",
        )
    )
    graph.add_edge(
        _edge(
            topics["enriched_orders"].node_id,
            schema_enriched.node_id,
            EdgeType.HAS_SCHEMA,
            role="value",
        )
    )
    graph.add_edge(
        _edge(
            topics["payments"].node_id,
            schema_payments.node_id,
            EdgeType.HAS_SCHEMA,
            role="value",
        )
    )

    # ── ksqlDB: orders + customers → enriched_orders (CONSUMES/PRODUCES)
    graph.add_edge(_edge(topics["orders"].node_id, ksql_enrich.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(topics["customers"].node_id, ksql_enrich.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(ksql_enrich.node_id, topics["enriched_orders"].node_id, EdgeType.PRODUCES))

    # ── ksqlDB: orders → order_status_updates (CONSUMES/PRODUCES)
    graph.add_edge(_edge(topics["orders"].node_id, ksql_status.node_id, EdgeType.CONSUMES))
    graph.add_edge(
        _edge(ksql_status.node_id, topics["order_status_updates"].node_id, EdgeType.PRODUCES)
    )

    # ── Flink: orders → orders_avro (CONSUMES/PRODUCES) ───────────────
    graph.add_edge(_edge(topics["orders"].node_id, flink_avro.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(flink_avro.node_id, topics["orders_avro"].node_id, EdgeType.PRODUCES))

    # ── Flink: orders → order_status_updates (CONSUMES/PRODUCES) ──────
    graph.add_edge(_edge(topics["orders"].node_id, flink_status.node_id, EdgeType.CONSUMES))
    graph.add_edge(
        _edge(flink_status.node_id, topics["order_status_updates"].node_id, EdgeType.PRODUCES)
    )

    # ── Sink: enriched_orders → Elasticsearch ─────────────────────────
    graph.add_edge(_edge(topics["enriched_orders"].node_id, conn_es.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(conn_es.node_id, ext_es.node_id, EdgeType.PRODUCES))

    # ── Sink: orders_avro → S3 ────────────────────────────────────────
    graph.add_edge(_edge(topics["orders_avro"].node_id, conn_s3.node_id, EdgeType.CONSUMES))
    graph.add_edge(_edge(conn_s3.node_id, ext_s3.node_id, EdgeType.PRODUCES))

    # ── Tableflow: topic → tableflow table → catalog (MATERIALIZES) ──
    graph.add_edge(_edge(topics["orders_avro"].node_id, tf_orders.node_id, EdgeType.MATERIALIZES))
    graph.add_edge(_edge(tf_orders.node_id, uc_orders.node_id, EdgeType.MATERIALIZES))

    graph.add_edge(_edge(topics["payments"].node_id, tf_payments.node_id, EdgeType.MATERIALIZES))
    graph.add_edge(_edge(tf_payments.node_id, glue_payments.node_id, EdgeType.MATERIALIZES))

    graph.add_edge(
        _edge(topics["enriched_orders"].node_id, tf_enriched.node_id, EdgeType.MATERIALIZES)
    )
    graph.add_edge(_edge(tf_enriched.node_id, uc_enriched.node_id, EdgeType.MATERIALIZES))

    # ── Shipments → Glue (Tableflow implied, no explicit TF node) ─────
    graph.add_edge(
        _edge(topics["shipments"].node_id, glue_shipments.node_id, EdgeType.MATERIALIZES)
    )

    # ── UC lineage: orders_avro TRANSFORMS → daily_order_summary ──────
    graph.add_edge(_edge(uc_orders.node_id, uc_daily_summary.node_id, EdgeType.TRANSFORMS))

    # ── Consumer groups (MEMBER_OF) ───────────────────────────────────
    graph.add_edge(
        _edge(
            topics["orders"].node_id,
            cg_nodes["orders-processing-cg"].node_id,
            EdgeType.MEMBER_OF,
        )
    )
    graph.add_edge(
        _edge(
            topics["enriched_orders"].node_id,
            cg_nodes["analytics-cg"].node_id,
            EdgeType.MEMBER_OF,
        )
    )
    graph.add_edge(
        _edge(
            topics["shipments"].node_id,
            cg_nodes["shipment-tracker-cg"].node_id,
            EdgeType.MEMBER_OF,
        )
    )
    graph.add_edge(
        _edge(
            topics["orders"].node_id,
            cg_nodes["flink-orders-consumer"].node_id,
            EdgeType.MEMBER_OF,
        )
    )

    return graph
