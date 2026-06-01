FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml watcher.py ./
RUN pip install --no-cache-dir .

CMD ["python", "-u", "watcher.py"]
