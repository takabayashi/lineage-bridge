FROM python:3.11-slim

WORKDIR /app

# Copy source and config
COPY pyproject.toml .
COPY lineage_bridge/ lineage_bridge/

# Install package
RUN pip install --no-cache-dir .

# Create data directory for output
RUN mkdir -p /app/data

# Streamlit port
EXPOSE 8501

# Default: run the UI
CMD ["streamlit", "run", "lineage_bridge/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
