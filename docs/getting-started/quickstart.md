# Quickstart Guide

Get your first lineage graph running in 5 minutes! This tutorial walks you through extracting and visualizing stream lineage from Confluent Cloud.

## What You'll Build

By the end of this guide, you'll have:

- An interactive lineage graph showing topics, connectors, and transformations
- A running Streamlit UI for exploring lineage
- Understanding of the extraction process

## Prerequisites

Before starting, ensure you have:

- [x] Python 3.11+ installed
- [x] A Confluent Cloud account with at least one Kafka cluster
- [x] Cloud-level API credentials (OrgAdmin or EnvironmentAdmin)

!!! tip "Don't Have Confluent Cloud?"
    Sign up for a free trial at [confluent.cloud](https://confluent.cloud). The free tier includes a Basic cluster perfect for testing.

## Step 1: Install LineageBridge

Choose your preferred installation method:

=== "uv (Recommended)"
    ```bash
    # Install uv if needed
    curl -LsSf https://astral.sh/uv/install.sh | sh
    
    # Clone and install
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge
    uv pip install -e .
    ```

=== "pip"
    ```bash
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge
    pip install -e .
    ```

=== "Make"
    ```bash
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge
    make install
    ```

**Verify installation:**

```bash
lineage-bridge-extract --help
```

You should see the help message for the extraction CLI.

## Step 2: Configure Credentials

### Create Your .env File

```bash
cp .env.example .env
```

### Add Your Confluent Cloud API Key

Edit `.env` and add your cloud-level API credentials:

```env
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

!!! question "Where Do I Get API Keys?"
    1. Log in to [Confluent Cloud](https://confluent.cloud)
    2. Go to **Administration → API Keys**
    3. Click **+ Add key**
    4. Select **Cloud resource** scope
    5. Copy the key and secret immediately

### Optional: Add Catalog Credentials

To enable data catalog integration, add these optional credentials:

=== "Databricks UC"
    ```env
    LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://your-workspace.databricks.com
    LINEAGE_BRIDGE_DATABRICKS_TOKEN=your-token
    LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=your-warehouse-id
    ```

=== "AWS Glue"
    ```env
    LINEAGE_BRIDGE_AWS_REGION=us-east-1
    ```
    Also ensure AWS credentials are configured via `aws configure` or environment variables.

=== "Google Data Lineage"
    ```env
    LINEAGE_BRIDGE_GCP_PROJECT_ID=my-gcp-project
    LINEAGE_BRIDGE_GCP_LOCATION=us-central1
    ```
    Also authenticate with `gcloud auth application-default login`.

## Step 3: Launch the UI

Start the Streamlit interface:

=== "uv"
    ```bash
    uv run streamlit run lineage_bridge/ui/app.py
    ```

=== "Make"
    ```bash
    make ui
    ```

The UI will automatically open at [http://localhost:8501](http://localhost:8501).

!!! success "First Launch"
    On first launch, you'll see the LineageBridge welcome screen with connection status.

## Step 4: Extract Lineage

### Select Your Environment

1. In the **sidebar**, you'll see a dropdown labeled **Environment**
2. Select the Confluent Cloud environment you want to extract from
3. Select a **Cluster** from the dropdown that appears

### Choose Extractors

The sidebar shows available extractors:

- **Kafka Topics & Consumer Groups** - Core topic inventory
- **Connect** - Source and sink connectors
- **ksqlDB** - Persistent queries and streams
- **Flink** - Flink SQL jobs
- **Schema Registry** - Schema associations
- **Stream Catalog** - Tags and business metadata
- **Tableflow** - Topic-to-table mappings
- **Metrics** - Throughput and lag metrics

By default, all extractors are enabled. For your first run, keep them all selected.

### Run Extraction

1. Click the **Extract Lineage** button at the bottom of the sidebar
2. Watch the progress indicators appear
3. Wait for extraction to complete (typically 10-30 seconds for a small cluster)

!!! tip "What's Happening?"
    LineageBridge is calling Confluent Cloud APIs to discover topics, connectors, schemas, and transformations, then building a graph of how data flows through your system.

### View Your Graph

Once extraction completes, you'll see:

- An **interactive graph visualization** in the main panel
- **Node colors** indicating different systems (Confluent blue, Databricks orange, etc.)
- **Edges** showing data flow relationships

### Explore the Graph

Try these interactions:

- **Drag nodes** to rearrange the layout
- **Scroll** to zoom in/out
- **Click a node** to see details in the right panel
- **Shift+drag** to select multiple nodes
- **Search** for nodes by name in the sidebar

## Step 5: Inspect Lineage Details

### Click on a Topic Node

When you click a Kafka topic, the detail panel shows:

- **Type**: KAFKA_TOPIC
- **Cluster**: Which cluster it belongs to
- **Attributes**: Partition count, replication factor, retention, etc.
- **Incoming Edges**: What produces to this topic (connectors, Flink jobs, etc.)
- **Outgoing Edges**: What consumes from this topic
- **Deep Link**: Click to open the topic in Confluent Cloud UI

### Click on a Connector Node

For a connector, you'll see:

- **Type**: CONNECTOR (source or sink)
- **Class**: The connector plugin class
- **Status**: Running, paused, failed
- **Configuration**: Connector-specific settings
- **Connected Topics**: What topics it reads from or writes to

### Click on a Transformation Node

For ksqlDB or Flink nodes:

- **SQL Statement**: The query or statement creating the transformation
- **Input Topics**: Source topics
- **Output Topics**: Destination topics

## Step 6: Export and Share

### Export Graph as JSON

1. Click the **Export Graph** button in the sidebar
2. The graph is saved as `lineage_graph.json` in your project directory
3. Share this file with teammates or load it later

### Deep Links

Every node includes a deep link to its resource in Confluent Cloud:

1. Click a node in the graph
2. In the detail panel, click the **View in Confluent Cloud** link
3. Your browser opens directly to that resource

## Next Steps

Congratulations! You've extracted and visualized your first lineage graph. Here's what to explore next:

### Learn More About the UI

- **[Streamlit UI Guide →](../user-guide/streamlit-ui.md)** - Full UI features and controls
- **[Graph Visualization →](../user-guide/graph-visualization.md)** - Advanced graph interactions
- **[Change Detection →](../user-guide/change-detection.md)** - Auto-refresh on changes

### Use CLI Tools

Try the command-line extraction:

```bash
# Extract and save to JSON
uv run lineage-bridge-extract

# Run the change-detection watcher
uv run lineage-bridge-watch
```

See **[CLI Tools Guide →](../user-guide/cli-tools.md)** for details.

### Integrate with Data Catalogs

Connect your lineage to external catalogs:

- **[Databricks Unity Catalog →](../catalog-integration/databricks-unity-catalog.md)**
- **[AWS Glue →](../catalog-integration/aws-glue.md)**
- **[Google Data Lineage →](../catalog-integration/google-data-lineage.md)**

### Automate with Docker

Deploy LineageBridge as a service:

```bash
# Run UI in Docker
make docker-ui

# Run extraction in Docker
make docker-extract

# Run watcher in Docker
make docker-watch
```

See **[Docker Deployment →](../how-to/docker-deployment.md)** for details.

### Explore the REST API

LineageBridge includes a REST API for programmatic access:

```bash
# Start the API server
make api
```

Then visit [http://localhost:8000/docs](http://localhost:8000/docs) for interactive API documentation.

See **[API Reference →](../api-reference/index.md)** for details.

## Common Scenarios

### Scenario 1: Multi-Cluster Lineage

If you have multiple clusters:

1. Extract lineage from the first cluster (select in UI)
2. Change the cluster dropdown to the next cluster
3. Click **Extract Lineage** again
4. The graph now shows combined lineage from both clusters

### Scenario 2: Focus on a Specific Topic

To explore lineage for a single topic:

1. Use the **Search** box in the sidebar
2. Type the topic name (e.g., `orders`)
3. Click the topic in search results
4. The graph highlights the topic and its neighbors
5. Click the topic to see full details

### Scenario 3: Finding Data Flow Paths

To trace data from source to destination:

1. Find your source connector in the graph
2. Follow the edges to see which topic it writes to
3. From that topic, see what consumes (ksqlDB, Flink, or sink connector)
4. Continue following edges to trace the full pipeline

## Troubleshooting

### No Graph Appears

If extraction completes but no graph appears:

1. Check the extraction log in the sidebar for errors
2. Verify you selected the correct environment and cluster
3. Ensure your API key has read permissions
4. Try extracting with fewer extractors enabled

### Authentication Errors

If you see "401 Unauthorized":

1. Verify your API key and secret in `.env`
2. Check that the key has not expired
3. Ensure the key has the correct scope (cloud-level, not cluster-level)

### Empty Graph

If the graph is empty:

1. Verify your cluster has topics (check in Confluent Cloud UI)
2. Try enabling only the **Kafka Topics** extractor first
3. Check the logs for API errors

### Performance Issues

If extraction is slow:

1. Disable **Metrics** extractor (it's the slowest)
2. Extract one cluster at a time
3. For large clusters (100+ topics), extraction may take a few minutes

For more help, see the full [Troubleshooting Guide](../troubleshooting/index.md).

## What's Next?

You've completed the quickstart! You now have a working LineageBridge installation and understand the basics of extraction and visualization.

To go deeper:

- **[User Guide](../user-guide/index.md)** - Comprehensive feature documentation
- **[Architecture](../architecture/index.md)** - How LineageBridge works under the hood
- **[How-To Guides](../how-to/index.md)** - Recipes for common tasks
- **[Contributing](../contributing/index.md)** - Help improve LineageBridge

## Get Help

If you run into issues:

- Check the [Troubleshooting Guide](../troubleshooting/index.md)
- Search [GitHub Issues](https://github.com/takabayashi/lineage-bridge/issues)
- Open a new issue with your error logs and configuration (redact secrets!)

Happy lineage mapping!
