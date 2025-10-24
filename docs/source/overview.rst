Project Overview
================

``agentic_py`` demonstrates a structured Python 3.11 project scaffold that
integrates common developer tooling, including linters, type checking,
documentation, and testing utilities. The project is intentionally small while
showing how modules can be organized into core functionality, data models,
services, and utilities.

Features
--------

* Pydantic-based configuration and data models for validation.
* Service orchestration layer for coordinating simple agent assignments.
* Unit and integration tests with pytest.
* Sphinx documentation configured for auto-documenting modules.

Getting Started
---------------

1. Install dependencies using Poetry: ``poetry install``.
2. Run ``pre-commit install`` to enable automated code quality checks.
3. Execute ``pytest`` to run the test suite.
4. Build documentation with ``sphinx-build docs/source docs/build``.

