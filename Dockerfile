FROM postgres:15

# Install Python, pgvector, and build dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    postgresql-15-pgvector \
    postgresql-plpython3-15 \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Use uv to manage the environment
RUN /uv/bin/uv sync --frozen --all-extras && /uv/bin/uv cache clean

# Set PYTHONPATH so PL/Python3u can find dependencies in the uv virtualenv
ENV PYTHONPATH=/app/.venv/lib/python3.13/site-packages:/app/src

# Copy source code and entrypoint

# Copy source code and entrypoint
COPY src/ ./src/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Ensure the postgres user can access the app and run uv
RUN chown -R postgres:postgres /app
USER postgres

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]
