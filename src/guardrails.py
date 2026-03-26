"""Data Pipeline Guardrails.

Production-ready guardrails for validating ETL/ELT pipeline freshness,
schema conformance, row-count thresholds, and data quality with
stakeholder-readable reporting.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class PipelineConfig:
    """Configuration for a data pipeline."""
    pipeline_name: str
    expected_schema: List[str]
    min_row_count: int = 100
    max_freshness_minutes: int = 60


@dataclass
class PipelineSnapshot:
    """Snapshot of a pipeline's current state."""
    pipeline_name: str
    actual_columns: List[str]
    row_count: int
    last_updated: datetime
    null_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class Verdict:
    """Result of a single guardrail check."""
    check_name: str
    passed: bool
    actual: str
    threshold: str
    message: str


@dataclass
class PipelineReport:
    """Full guardrail report for a pipeline."""
    pipeline_name: str
    verdicts: List[Verdict] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(v.passed for v in self.verdicts)

    @property
    def summary(self) -> str:
        status = "PASS" if self.all_passed else "FAIL"
        passed = sum(1 for v in self.verdicts if v.passed)
        return f"[{status}] {self.pipeline_name}: {passed}/{len(self.verdicts)} checks passed"


def check_freshness(snapshot: PipelineSnapshot, config: PipelineConfig, now: Optional[datetime] = None) -> Verdict:
    """Check if pipeline data is fresh enough."""
    now = now or datetime.utcnow()
    age = now - snapshot.last_updated
    age_minutes = age.total_seconds() / 60
    max_min = config.max_freshness_minutes
    passed = age_minutes <= max_min
    return Verdict(
        check_name="freshness",
        passed=passed,
        actual=f"{age_minutes:.0f} min",
        threshold=f"{max_min} min",
        message=f"Data age {age_minutes:.0f} min {'within' if passed else 'exceeds'} {max_min} min limit",
    )


def check_schema(snapshot: PipelineSnapshot, config: PipelineConfig) -> Verdict:
    """Check if actual columns match expected schema."""
    expected = set(config.expected_schema)
    actual = set(snapshot.actual_columns)
    missing = expected - actual
    passed = len(missing) == 0
    return Verdict(
        check_name="schema",
        passed=passed,
        actual=f"{len(actual)} cols",
        threshold=f"{len(expected)} expected",
        message=f"Schema {'matches' if passed else 'missing: ' + ', '.join(sorted(missing))}",
    )


def check_row_count(snapshot: PipelineSnapshot, config: PipelineConfig) -> Verdict:
    """Check if row count meets minimum threshold."""
    passed = snapshot.row_count >= config.min_row_count
    return Verdict(
        check_name="row_count",
        passed=passed,
        actual=str(snapshot.row_count),
        threshold=str(config.min_row_count),
        message=f"Row count {snapshot.row_count} {'meets' if passed else 'below'} minimum {config.min_row_count}",
    )


def check_nulls(snapshot: PipelineSnapshot, max_null_pct: float = 0.10) -> Verdict:
    """Check that no column exceeds the null percentage threshold."""
    if snapshot.row_count == 0:
        return Verdict("nulls", False, "0 rows", f"{max_null_pct:.0%}", "Cannot check nulls on empty dataset")
    worst_col = ""
    worst_pct = 0.0
    for col, null_count in snapshot.null_counts.items():
        pct = null_count / snapshot.row_count
        if pct > worst_pct:
            worst_pct = pct
            worst_col = col
    passed = worst_pct <= max_null_pct
    return Verdict(
        check_name="nulls",
        passed=passed,
        actual=f"{worst_col}: {worst_pct:.1%}" if worst_col else "0%",
        threshold=f"{max_null_pct:.0%}",
        message=f"Null rate {'within' if passed else 'exceeds'} {max_null_pct:.0%} threshold"
        + (f" (worst: {worst_col} at {worst_pct:.1%})" if worst_col else ""),
    )


def evaluate_pipeline(snapshot: PipelineSnapshot, config: PipelineConfig, now: Optional[datetime] = None) -> PipelineReport:
    """Run all guardrail checks for a pipeline."""
    report = PipelineReport(pipeline_name=snapshot.pipeline_name)
    report.verdicts.append(check_freshness(snapshot, config, now))
    report.verdicts.append(check_schema(snapshot, config))
    report.verdicts.append(check_row_count(snapshot, config))
    report.verdicts.append(check_nulls(snapshot))
    return report


def generate_markdown_report(reports: List[PipelineReport]) -> str:
    """Generate a stakeholder-readable markdown report."""
    lines = ["# Data Pipeline Guardrail Report", ""]
    for report in reports:
        status = "PASS" if report.all_passed else "FAIL"
        lines.append(f"## {report.pipeline_name} \u2014 {status}")
        lines.append("")
        lines.append("| Check | Result | Actual | Threshold | Detail |")
        lines.append("|-------|--------|--------|-----------|--------|")
        for v in report.verdicts:
            icon = "PASS" if v.passed else "FAIL"
            lines.append(f"| {v.check_name} | {icon} | {v.actual} | {v.threshold} | {v.message} |")
        lines.append("")
    return "\n".join(lines)
