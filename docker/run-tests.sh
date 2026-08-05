#!/usr/bin/env bash
set -euo pipefail

PY4J=$(ls /opt/spark/python/lib/py4j-*-src.zip | head -1)
export PYTHONPATH="/opt/spark/python:${PY4J}:/opt/app/src"

python3 -m pip install -q pytest
exec python3 -m pytest tests -q