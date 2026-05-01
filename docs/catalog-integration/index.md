# Catalog Integration Overview

LineageBridge bridges Confluent stream lineage into external data catalogs using an extensible provider pattern. Each catalog provider implements a standard interface to build nodes, enrich metadata, push lineage, and generate deep links.

## Supported Catalogs

| Catalog | Provider | Features | Status |
|---------|----------|----------|--------|
| **Databricks Unity Catalog** | `DatabricksUCProvider` | Enrichment, lineage push (SQL), lineage discovery | Production |
| **AWS Glue Data Catalog** | `GlueCatalogProvider` | Enrichment, lineage push (parameters) | Production |
| **Google Data Lineage** | `GoogleLineageProvider` | Enrichment, lineage push (OpenLineage) | Production |

## How Catalog Integration Works

Catalog integration happens during lineage extraction in two phases:

### Phase 4: Tableflow Mapping

The Tableflow client queries the Confluent Tableflow API to discover topic-to-table mappings. For each catalog integration configured in Tableflow:

1. **Build Node**: The provider creates a catalog table node (e.g., `UC_TABLE`, `GLUE_TABLE`, `GOOGLE_TABLE`)
2. **Build Edge**: A `MATERIALIZES` edge is created from the `TABLEFLOW_TABLE` node to the catalog table node
3. **Add to Graph**: The node and edge are added to the lineage graph

### Phase 4b: Catalog Enrichment

After all tableflow nodes are built, each provider's `enrich()` method runs in parallel to backfill metadata:

- Fetch table schema, owner, storage location, etc. from the catalog's API
- Discover downstream derived tables (Databricks UC only)
- Merge enriched attributes into existing nodes

### Lineage Push (Optional)

After extraction completes, you can push lineage metadata back to the catalog via the UI or API:

- **Databricks UC**: Sets table properties (`lineage_bridge.*`), comments, and optionally creates a bridge table
- **AWS Glue**: Updates table parameters and description
- **Google Data Lineage**: Pushes OpenLineage events to the Data Lineage API

## Provider Comparison

### Databricks Unity Catalog

**Strengths:**
- Full metadata enrichment via REST API
- Downstream lineage discovery (finds derived tables automatically)
- Rich lineage push with table properties, comments, and bridge table
- Deep links to workspace UI

**Configuration:**
```env
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi...
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456  # For lineage push
```

**Use Cases:**
- Lakehouse architectures with Databricks
- Teams using Unity Catalog as the primary data catalog
- Environments with complex table-to-table transformations

### AWS Glue Data Catalog

**Strengths:**
- Native integration with AWS analytics services (Athena, Redshift Spectrum, EMR)
- Simple enrichment via boto3
- Table parameters for metadata storage

**Configuration:**
```env
LINEAGE_BRIDGE_AWS_REGION=us-east-1
# AWS credentials via environment variables or ~/.aws/credentials
```

**Use Cases:**
- AWS-centric data platforms
- S3-based data lakes with Glue crawlers
- Integration with AWS analytics tools

### Google Data Lineage

**Strengths:**
- Native OpenLineage support (no custom metadata format needed)
- BigQuery metadata enrichment
- Integration with Google Cloud Data Catalog and Dataplex

**Configuration:**
```env
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us  # or us-central1, etc.
# Google credentials via Application Default Credentials (gcloud auth application-default login)
```

**Use Cases:**
- Google Cloud data platforms
- BigQuery-centric analytics
- Organizations using Google Data Catalog

## Feature Matrix

| Feature | Databricks UC | AWS Glue | Google Lineage |
|---------|--------------|----------|----------------|
| **Build Node** | UC_TABLE | GLUE_TABLE | GOOGLE_TABLE |
| **Enrich Metadata** | Tables API | get_table | BigQuery API |
| **Push Lineage** | SQL (properties, comments, bridge table) | update_table (parameters, description) | OpenLineage events |
| **Discover Derived Tables** | Yes (lineage API) | No | No |
| **Deep Links** | Yes | Yes | Yes |
| **Authentication** | Token | boto3 / IAM | Application Default Credentials |

## Next Steps

- [Databricks Unity Catalog Setup](databricks-unity-catalog.md) - Configure UC integration
- [AWS Glue Setup](aws-glue.md) - Configure Glue integration
- [Google Data Lineage Setup](google-data-lineage.md) - Configure Google integration
- [Adding New Catalogs](adding-new-catalogs.md) - Developer guide for custom providers
