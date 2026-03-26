"""Tests for Data Pipeline Guardrails."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from guardrails import (
    PipelineConfig,
    PipelineSnapshot,
    check_freshness,
    check_nulls,
    evaluate_pipeline,
    generate_markdown_report,
)


def _make_config():
    return PipelineConfig(
        pipeline_name="orders_daily",
        expected_schema=["id", "customer_id", "amount", "created_at"],
        min_row_count=100,
        max_freshness_minutes=60,
    )


def _make_snapshot(columns=None, row_count=500, age_minutes=30, null_counts=None):
    columns = columns or ["id", "customer_id", "amount", "created_at"]
    now = datetime.utcnow()
    return PipelineSnapshot(
        pipeline_name="orders_daily",
        actual_columns=columns,
        row_count=row_count,
        last_updated=now - timedelta(minutes=age_minutes),
        null_counts=null_counts or {},
    ), now


def test_healthy_pipeline_passes():
    """A pipeline within all thresholds should pass all checks."""
    config = _make_config()
    snapshot, now = _make_snapshot()
    report = evaluate_pipeline(snapshot, config, now)
    assert report.all_passed
    assert len(report.verdicts) == 4


def test_stale_data_fails_freshness():
    """Data older than the freshness limit should fail."""
    config = _make_config()
    snapshot, now = _make_snapshot(age_minutes=120)
    report = evaluate_pipeline(snapshot, config, now)
    freshness = [v for v in report.verdicts if v.check_name == "freshness"][0]
    assert not freshness.passed


def test_missing_columns_fails_schema():
    """Missing expected columns should fail schema check."""
    config = _make_config()
    snapshot, now = _make_snapshot(columns=["id", "amount"])
    report = evaluate_pipeline(snapshot, config, now)
    schema = [v for v in report.verdicts if v.check_name == "schema"][0]
    assert not schema.passed
    assert "customer_id" in schema.message


def test_high_nulls_fails():
    """A column with >10% nulls should fail the null check."""
    snapshot, _ = _make_snapshot(row_count=100, null_counts={"amount": 20})
    verdict = check_nulls(snapshot, max_null_pct=0.10)
    assert not verdict.passed


def test_report_generation():
    """Markdown report should contain pipeline name and check results."""
    config = _make_config()
    snapshot, now = _make_snapshot()
    report = evaluate_pipeline(snapshot, config, now)
    md = generate_markdown_report([report])
    assert "# Data Pipeline Guardrail Report" in md
    assert "orders_daily" in md
    assert "PASS" in md
