# Zerobus SDKs

Monorepo for Databricks Zerobus Ingest SDKs.

## Disclaimer

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): These SDKs are supported for production use cases and are available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDKs. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you. Please [file issues](https://github.com/databricks/zerobus-sdk/issues), and we will address them.

## What is Zerobus?

Zerobus is a high-throughput streaming service for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.

## SDKs

| Language | Directory | Package |
|----------|-----------|---------|
| Rust | [`rust/`](rust/) | [`databricks-zerobus-ingest-sdk`](https://crates.io/crates/databricks-zerobus-ingest-sdk) |
| Python | `python/` | *coming soon* |
| Go | `go/` | *coming soon* |
| TypeScript | `typescript/` | *coming soon* |
| Java | `java/` | *coming soon* |

## Prerequisites

Before using any SDK, you need the following:

### 1. Workspace URL and Workspace ID

After logging into your Databricks workspace, look at the browser URL:

```
https://<databricks-instance>.cloud.databricks.com/o=<workspace-id>
```

- **Workspace URL**: The part before `/o=` (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`)
- **Workspace ID**: The part after `/o=` (e.g., `1234567890123456`)

> **Note:** The examples above show AWS endpoints (`.cloud.databricks.com`). For Azure deployments, the workspace URL will be `https://<databricks-instance>.azuredatabricks.net`.

### 2. Create a Delta Table

Create a table using Databricks SQL:

```sql
CREATE TABLE <catalog_name>.default.<table_name> (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Replace `<catalog_name>` with your catalog name (e.g., `main`).

### 3. Create a Service Principal

1. Navigate to **Settings > Identity and Access** in your Databricks workspace
2. Click **Service principals** and create a new service principal
3. Generate a new secret for the service principal and save it securely
4. Grant the following permissions:
   - `USE_CATALOG` on the catalog (e.g., `main`)
   - `USE_SCHEMA` on the schema (e.g., `default`)
   - `MODIFY` and `SELECT` on the table

Grant permissions using SQL:

```sql
-- Grant catalog permission
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;

-- Grant schema permission
GRANT USE SCHEMA ON SCHEMA <catalog_name>.default TO `<service-principal-application-id>`;

-- Grant table permissions
GRANT SELECT, MODIFY ON TABLE <catalog_name>.default.<table_name> TO `<service-principal-application-id>`;
```

The service principal's **Application ID** is your OAuth **Client ID**, and the generated secret is your **Client Secret**.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Each SDK also has its own contributing guide with language-specific setup instructions.

## License

This project is licensed under the Databricks License. See [LICENSE](LICENSE) for the full text.
