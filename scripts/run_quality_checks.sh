#!/usr/bin/env bash
set -euo pipefail

# Run formatting, linting, type checking, and tests in sequence.
poetry run ruff format src tests
poetry run ruff check src tests
poetry run mypy src
# poetry run pytest --cov=agentic_py --cov-report=term-missing
coverage run -m pytest
coverage report -m
