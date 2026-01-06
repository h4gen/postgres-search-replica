# Build stage for Next.js webapp
FROM node:20-bookworm AS webapp-builder
WORKDIR /webapp
COPY webapp/package.json webapp/package-lock.json ./
RUN npm ci
COPY webapp/ ./
RUN npm run build

# Final image
FROM postgres:15-bookworm

# Install Python, pgvector, build dependencies and Node.js
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    postgresql-15-pgvector \
    postgresql-plpython3-15 \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Use uv to manage the environment
RUN /uv/bin/uv sync --frozen --all-extras && /uv/bin/uv cache clean

# Install the pgai extension files into the Postgres system
RUN /app/.venv/bin/python3 -m pgai.cli install

# Set PYTHONPATH so PL/Python3u can find dependencies in the virtualenv
ENV PYTHONPATH=/app/.venv/lib/python3.13/site-packages:/app/src

# Copy source code and entrypoint
COPY src/ ./src/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Copy webapp static files from builder
COPY --from=webapp-builder /webapp/public ./webapp/public
COPY --from=webapp-builder /webapp/.next ./webapp/.next
COPY --from=webapp-builder /webapp/package.json ./webapp/package.json
COPY --from=webapp-builder /webapp/node_modules ./webapp/node_modules

# Ensure the postgres user can access the app and run uv
RUN chown -R postgres:postgres /app

# Create directory for local data and set permissions
RUN mkdir -p /var/lib/postgresql/.local/share/pg-search-replica && \
    chown -R postgres:postgres /var/lib/postgresql/.local

USER postgres

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]
