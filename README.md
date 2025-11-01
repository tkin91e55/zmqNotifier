zmqnotifier
==========

``zmqnotifier`` is a reference implementation of a modern Python 3.11 project
that demonstrates how to structure code, documentation, and tooling when
using agentic agent. The repository highlights an opinionated
setup featuring Poetry for dependency management, Pydantic for validation,
pytest for testing, and Sphinx for documentation.

This project template serves similar purpose to cookiecutter, but less complexity.
Aim good enough for bootstrapping new projects.

Project Structure
-----------------

The src/ and tests/ does nothing actually

```
zmqnotifier/
├── src/zmqnotifier/          # Application package
│   ├── core/                # Core functionality (registries, coordination)
│   ├── models/              # Pydantic models and schemas
│   ├── services/            # Business orchestration logic
│   └── utils/               # Shared helpers
├── tests/                   # Pytest suites
│   ├── unit/                # Unit-level checks
│   └── integration/         # End-to-end style tests
├── docs/                    # Sphinx documentation source
├── scripts/                 # Project automation scripts
├── .github/workflows/       # CI configuration
├── pyproject.toml           # Poetry project definition
└── README.md
```

Quick Start
-----------

```bash
poetry install
poetry run pre-commit install
poetry run pytest
poetry run sphinx-build docs/source docs/build
```

Tooling Overview
----------------

# Type checking
  * `mypy .` or `mypy {some_module}`
  - [ ]  `mypy.ini` included for configuration
# Linting
  * `ruff check` , `ruff check --fix`
  * `pylint . --output-format=colorized`
# Formatting
  - **Formatting:** ``ruff format`` and ``pydocstringformatter``
  * `ruff format` or dry run `ruff format --diff`
  * `pydocstringformatter -r .` or dry run `pydocstringformatter -r . --check`
# Testing
  * pytest .
  * coverage -m pytest .
  * coverage report -m
  * tox -e py311
# Docs
  Not really working well, need more thoughts and consultation
  * `sphinx-build -b html docs/source docs/build`

Environment Variables
---------------------

Copy ``.env.example`` to ``.env`` and adjust fields as needed:

```bash
cp .env.example .env
```

Development Workflow
--------------------

1. Branch from ``develop`` using ``feature/*`` or ``bugfix/*`` naming.
2. Implement changes with comprehensive tests and documentation updates.
3. Run ``scripts/run_quality_checks.sh`` prior to committing.
4. Ensure CI (lint, type check, tests, docs) reports success before merging.

License
-------

Released under the MIT License. See ``LICENSE`` for details.
