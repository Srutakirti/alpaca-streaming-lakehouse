FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 && rm -rf /var/lib/apt/lists/*
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY extract/pyproject.toml extract/
RUN uv sync --package extract
COPY extract/extractor.py extract/
CMD ["uv", "run", "--package", "extract", "python", "extract/extractor.py"]
