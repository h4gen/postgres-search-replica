FROM postgres:15

# Install Python, pgvector, and build dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    postgresql-15-pgvector \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml .

# Use uv to manage the environment
# uv sync will create a .venv in the working directory
RUN /uv/bin/uv sync

# Copy source code and entrypoint
COPY src/ ./src/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]

