FROM apache/beam_python3.13_sdk:2.72.0

WORKDIR /pipeline

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY pipeline/ ./pipeline/
COPY config/ ./config/
