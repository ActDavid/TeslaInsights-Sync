FROM python:3.12-alpine3.20

LABEL \
  io.hass.version="0.3.1" \
  io.hass.type="app" \
  io.hass.arch="aarch64|amd64"

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY teslainsights_sync/ ./teslainsights_sync/

CMD [ "python", "-m", "teslainsights_sync" ]
