# Script to run the health checker application
set -e

export PYTHONUNBUFFERED=1

exec python -m app.main
