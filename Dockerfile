FROM python:3.12-slim

WORKDIR /app

ARG PUDA_VERSION=0.0.26
RUN apt-get update && apt-get install -y --no-install-recommends curl tar \
    && curl -fsSL "https://github.com/PUDAP/puda/releases/download/v${PUDA_VERSION}/puda_linux_x86_64.tar.gz" \
       | tar -xz -C /usr/local/bin puda \
    && chmod +x /usr/local/bin/puda \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir influxdb3-python

COPY watcher.py .

CMD ["python", "-u", "watcher.py"]
