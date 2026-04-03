FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy source code
COPY lineage_bridge/ lineage_bridge/

# Re-install with source so entry points work
RUN pip install --no-cache-dir -e .

# Create data directory for output
RUN mkdir -p /app/data

# Streamlit port
EXPOSE 8501

# Default: run the UI
CMD ["streamlit", "run", "lineage_bridge/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
