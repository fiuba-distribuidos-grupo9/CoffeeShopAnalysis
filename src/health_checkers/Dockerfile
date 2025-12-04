FROM python:3.13-slim

WORKDIR /app

RUN pip install --upgrade pip && pip3 install docker==7.1.0

COPY src/health_checkers/app /app/app
COPY src/shared /app/shared
COPY src/health_checkers/scripts/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
