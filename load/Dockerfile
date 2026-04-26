FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 libpq5 && rm -rf /var/lib/apt/lists/*
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY load/pyproject.toml load/
RUN uv sync --package load
COPY load/subscriber.py load/
CMD ["uv", "run", "--package", "load", "python", "load/subscriber.py"]
