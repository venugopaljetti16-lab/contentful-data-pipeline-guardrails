# Contentful Data Pipeline Guardrails

Production-ready guardrails for validating ETL/ELT pipeline freshness, schema conformance, row-count thresholds, and data quality with stakeholder-readable reporting.

## What It Demonstrates

- **Freshness validation** ensuring pipeline data stays within acceptable age limits
- **Schema conformance** checking actual columns against expected schema definitions
- **Row count enforcement** with configurable minimum thresholds
- **Null rate monitoring** detecting columns exceeding acceptable null percentages
- **Stakeholder-readable reports** in markdown format with per-check breakdowns

## Quick Start

```bash
pip install pytest
pytest tests/ -v
```

## Architecture

```
src/guardrails.py          # Core evaluation logic, quality checks, and reporting
tests/test_guardrails.py   # 5 tests covering freshness, schema, nulls, and reporting
```

## Tech Stack

Python, pytest, dataclasses, datetime, type hints
