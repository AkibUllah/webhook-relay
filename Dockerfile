# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /srv/relay

COPY pyproject.toml requirements.txt README.md ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY webhook_relay ./webhook_relay
RUN pip install --no-deps -e .

RUN addgroup --system relay && adduser --system --ingroup relay relay && chown -R relay:relay /srv/relay
USER relay

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status == 200 else 1)"

# Default: API with the in-process worker. Override CMD to run a dedicated worker:
#   python -m webhook_relay.worker
CMD ["uvicorn", "webhook_relay.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
