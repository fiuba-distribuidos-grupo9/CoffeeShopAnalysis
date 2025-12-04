#!/bin/bash
set -e

export PYTHONUNBUFFERED=1

exec python -m app.main
