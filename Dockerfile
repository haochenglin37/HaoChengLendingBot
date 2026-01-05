FROM python:3.11-slim AS base
LABEL maintainer="Mika Lending Bot"

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    LENDINGBOT_CONFIG=/config/default.cfg

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy source code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash lendingbot && \
    chown -R lendingbot:lendingbot /app

USER lendingbot

# Directories commonly mounted from the host
VOLUME ["/config", "/app/market_data", "/app/www"]

EXPOSE 8000

# Use env-configurable path for default.cfg
CMD ["sh", "-c", "python lendingbot.py -cfg ${LENDINGBOT_CONFIG}"]
