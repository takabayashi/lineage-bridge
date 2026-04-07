FROM python:3.11-slim AS builder

WORKDIR /app

# Copy source and config
COPY pyproject.toml .
COPY lineage_bridge/ lineage_bridge/

# Build the wheel
RUN pip install --no-cache-dir build && \
    python -m build --wheel --outdir /app/dist

# ── Runtime stage ────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Install the wheel (brings in all dependencies)
COPY --from=builder /app/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# Copy source for Streamlit (needs the actual .py files to run)
COPY lineage_bridge/ lineage_bridge/

# Create data directory for output
RUN mkdir -p /app/data

# Streamlit config: disable telemetry, set server defaults
RUN mkdir -p /root/.streamlit && \
    printf '[server]\nheadless = true\nport = 8501\naddress = "0.0.0.0"\n\n[browser]\ngatherUsageStats = false\n' \
    > /root/.streamlit/config.toml

EXPOSE 8501

# Default: run the UI
CMD ["streamlit", "run", "lineage_bridge/ui/app.py"]
