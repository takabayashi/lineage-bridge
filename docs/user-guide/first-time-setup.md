# First-Time Setup

LineageBridge makes it easy to get started with a guided welcome dialog that appears automatically when you first launch the UI without credentials.

## Welcome Dialog

When you open LineageBridge for the first time (or after running the quickstart script), you'll see a welcome dialog offering three options:

### Option 1: Enter Credentials

Connect to your Confluent Cloud environment:

1. **Cloud API Key**: Your Confluent Cloud API key (org or environment-level)
2. **Cloud API Secret**: The corresponding secret

**Where to get credentials:**
- [Confluent Cloud Console](https://confluent.cloud/settings/api-keys) → Settings → API Keys
- Or via CLI: `confluent api-key create --resource cloud`

The dialog will:
- ✓ Save credentials to your `.env` file
- ✓ Set them in the current environment
- ✓ Reload the app automatically
- ✓ Enable environment/cluster discovery

### Option 2: Skip for Now

Dismiss the dialog and explore the UI without credentials:

- The sidebar connection section remains available
- You can add credentials later via the sidebar
- Perfect for browsing the interface first

### Option 3: Load Demo Graph

Instantly load a sample lineage graph:

- No credentials needed
- Explore a pre-built graph with:
  - Kafka topics
  - Connectors (source & sink)
  - Flink jobs
  - ksqlDB queries
  - Tableflow tables
  - Catalog tables (Unity Catalog, AWS Glue, BigQuery)
- Perfect for demos and evaluation

## Security

**Credentials are stored locally:**
- Saved to `.env` in your project directory
- Never sent anywhere except to Confluent Cloud APIs during extraction
- The `.env` file should be added to `.gitignore` (already configured)

**Encrypted cache:**
LineageBridge also maintains an encrypted cache at `~/.lineage_bridge/cache.json` for per-environment and per-cluster credentials. This uses Fernet symmetric encryption with a machine-local key.

## Re-triggering the Dialog

The welcome dialog only appears once per session. To see it again:

1. **Remove credentials from .env:**
   ```bash
   # Backup first
   mv .env .env.backup
   
   # Start the UI
   make ui
   ```

2. **Or clear session state:**
   - Refresh the browser (Ctrl+R / Cmd+R)
   - The dialog checks for credentials on each page load

## Adding Credentials Later

If you skip the welcome dialog, you can always add credentials via the sidebar:

1. Open the **Setup** expander in the sidebar
2. Enter your Cloud API Key and Secret
3. Click **Connect to Confluent Cloud**

## Troubleshooting

### "Credentials saved but discovery failed"

**Cause:** Invalid or insufficient permissions on the API key.

**Fix:**
1. Verify the key has **CloudClusterAdmin** or **OrganizationAdmin** role
2. Check for typos in the key/secret
3. Create a new key with proper permissions:
   ```bash
   confluent api-key create --resource cloud --description "LineageBridge"
   ```

### "Dialog doesn't appear"

**Cause:** Credentials already exist in `.env` or environment variables.

**Fix:**
- Check `.env` for `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
- Check environment: `env | grep LINEAGE_BRIDGE_CONFLUENT`
- If found, the app considers you already configured

### "Can't save credentials - permission denied"

**Cause:** No write permission to the project directory.

**Fix:**
```bash
# Check current directory permissions
ls -la .env

# Make directory writable
chmod u+w .
```

## Next Steps

After adding credentials:

1. **Discover environments** → Sidebar shows your Confluent environments
2. **Select scope** → Choose which environments/clusters to extract
3. **Extract lineage** → Click "Extract Lineage" in the sidebar
4. **Explore the graph** → Visualize your data flows

See the [Quickstart Guide](../getting-started/quickstart.md) for a complete walkthrough.
