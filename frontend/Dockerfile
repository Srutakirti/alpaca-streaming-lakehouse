# ---- Stage 1: build SPA ----
FROM node:22-slim AS web-builder
WORKDIR /web
COPY frontend/web/package.json frontend/web/package-lock.json ./
RUN npm ci
COPY frontend/web/ ./
RUN npm run build

# ---- Stage 2: python runtime ----
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 && rm -rf /var/lib/apt/lists/*
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY frontend/pyproject.toml frontend/
RUN uv sync --package frontend

COPY frontend/__init__.py frontend/server.py frontend/
COPY frontend/app/ frontend/app/

COPY --from=web-builder /web/dist/ frontend/app/static/

ENV PORT=8080
EXPOSE 8080
CMD ["uv", "run", "--package", "frontend", "python", "frontend/server.py"]
