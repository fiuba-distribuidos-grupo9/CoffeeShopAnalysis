FROM python:3.13-slim

# WORKDIR /app

COPY src/health_checkers/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY src/health_checkers/app /app/app
COPY src/shared /app/shared
COPY src/health_checkers/scripts/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# ENV PYTHONUNBUFFERED=1

# ENTRYPOINT ["/app/entrypoint.sh"]
