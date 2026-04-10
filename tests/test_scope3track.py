"""Tests for scope3track — carbon and Scope 3 emissions tracking."""
import asyncio
import json
import pytest
from datetime import datetime

from scope3track import (
    EmissionCalculator,
    EmissionFactorRegistry,
    EmissionScope,
    Scope3Category,
    EmissionEntry,
    EmissionReport,
    EmissionCache,
    EmissionPipeline,
    EmissionValidator,
    EmissionRule,
    EmissionDriftDetector,
    EmissionReportExporter,
    EmissionDiff,
    diff_entries,
    stream_entries,
    entries_to_ndjson,
    AuditLog,
    PIIScrubber,
    RateLimiter,
    CancellationToken,
    CalculationError,
    default_registry,
)

P_START = datetime(2025, 1, 1)
P_END = datetime(2025, 3, 31)


def make_entry(eid="E1", scope=EmissionScope.SCOPE3, kg=100.0, supplier_id=None) -> EmissionEntry:
    return EmissionEntry(
        entry_id=eid,
        scope=scope,
        source="test_source",
        activity_amount=1000,
        activity_unit="kg",
        emission_factor=kg / 1000,
        emissions_kg_co2e=kg,
        period_start=P_START,
        period_end=P_END,
        supplier_id=supplier_id,
    )


# ─── Registry ─────────────────────────────────────────────────────────────────

def test_default_registry_has_factors():
    factor = default_registry.get("kwh_electricity_us")
    assert factor > 0


def test_registry_register_custom():
    reg = EmissionFactorRegistry()
    reg.register("custom_factor", 1.234)
    assert abs(reg.get("custom_factor") - 1.234) < 1e-9


def test_registry_unknown_factor_raises():
    reg = EmissionFactorRegistry()
    with pytest.raises(CalculationError):
        reg.get("nonexistent_factor_xyz")


def test_registry_negative_factor_raises():
    reg = EmissionFactorRegistry()
    with pytest.raises(CalculationError):
        reg.register("bad", -1.0)


def test_registry_list_keys():
    reg = EmissionFactorRegistry()
    keys = reg.list_keys()
    assert "kwh_natural_gas" in keys


# ─── Calculator ───────────────────────────────────────────────────────────────

def test_calculate_with_factor_key():
    calc = EmissionCalculator()
    entry = calc.calculate(
        "E1", EmissionScope.SCOPE2, "Office electricity", 10000, "kwh",
        factor_key="kwh_electricity_us",
        period_start=P_START, period_end=P_END,
    )
    assert entry.emissions_kg_co2e > 0
    assert entry.scope == EmissionScope.SCOPE2


def test_calculate_with_custom_factor():
    calc = EmissionCalculator()
    entry = calc.calculate(
        "E2", EmissionScope.SCOPE1, "Company car", 5000, "km",
        custom_factor=0.21,
        period_start=P_START, period_end=P_END,
    )
    assert abs(entry.emissions_kg_co2e - 5000 * 0.21) < 1e-6


def test_calculate_no_factor_raises():
    calc = EmissionCalculator()
    with pytest.raises(CalculationError):
        calc.calculate("E3", EmissionScope.SCOPE3, "test", 100, "unit", period_start=P_START, period_end=P_END)


def test_build_report():
    calc = EmissionCalculator()
    entries = [
        calc.calculate("s1", EmissionScope.SCOPE1, "Gas", 1000, "kwh", factor_key="kwh_natural_gas", period_start=P_START, period_end=P_END),
        calc.calculate("s2", EmissionScope.SCOPE2, "Elec", 5000, "kwh", factor_key="kwh_electricity_us", period_start=P_START, period_end=P_END),
        calc.calculate("s3", EmissionScope.SCOPE3, "Travel", 10000, "km", factor_key="km_passenger_car", category=Scope3Category.EMPLOYEE_COMMUTING, period_start=P_START, period_end=P_END),
    ]
    report = calc.build_report("ACME", 2025, entries)
    assert report.scope1_kg_co2e > 0
    assert report.scope2_kg_co2e > 0
    assert report.scope3_kg_co2e > 0
    assert report.total_t_co2e == pytest.approx(report.total_kg_co2e / 1000, rel=1e-5)


def test_report_summary():
    calc = EmissionCalculator()
    entry = calc.calculate("s1", EmissionScope.SCOPE1, "Gas", 1000, "kwh", factor_key="kwh_natural_gas", period_start=P_START, period_end=P_END)
    report = calc.build_report("CORP", 2025, [entry])
    s = report.summary()
    assert s["company_id"] == "CORP"
    assert s["reporting_year"] == 2025


# ─── Cache ────────────────────────────────────────────────────────────────────

def test_cache_set_get():
    cache = EmissionCache(max_size=10, ttl_seconds=60)
    cache.set("k1", "v1")
    assert cache.get("k1") == "v1"
    assert cache.get("missing") is None


def test_cache_memoize():
    cache = EmissionCache()
    calls = [0]

    @cache.memoize
    def calc(x):
        calls[0] += 1
        return x * 2.0

    assert calc(10) == 20.0
    assert calc(10) == 20.0
    assert calls[0] == 1


def test_cache_stats():
    cache = EmissionCache(max_size=10, ttl_seconds=60)
    cache.set("k", "v")
    cache.get("k")
    cache.get("miss")
    s = cache.stats()
    assert s["hits"] == 1
    assert s["misses"] == 1


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def test_pipeline_filter_by_scope():
    entries = [make_entry(f"E{i}", scope=EmissionScope.SCOPE3) for i in range(3)]
    entries.append(make_entry("E4", scope=EmissionScope.SCOPE1))
    pipeline = EmissionPipeline().filter(lambda e: e.scope == EmissionScope.SCOPE3)
    result = pipeline.run(entries)
    assert all(e.scope == EmissionScope.SCOPE3 for e in result)


def test_pipeline_audit():
    entries = [make_entry("E1")]
    pipeline = EmissionPipeline().filter(lambda e: True, name="pass")
    pipeline.run(entries)
    log = pipeline.audit_log()
    assert log[0]["ok"] is True


def test_pipeline_async():
    entries = [make_entry("E1")]
    pipeline = EmissionPipeline().filter(lambda e: True)
    result = asyncio.run(pipeline.arun(entries))
    assert len(result) == 1


# ─── Validator ────────────────────────────────────────────────────────────────

def test_validator_max_kg():
    validator = EmissionValidator()
    validator.add_rule(EmissionRule("max_kg", 50, "Too high"))
    entry = make_entry(kg=200.0)
    ok, errors = validator.validate(entry)
    assert not ok
    assert any("Too high" in e for e in errors)


def test_validator_required_supplier():
    validator = EmissionValidator()
    validator.add_rule(EmissionRule("required_supplier", True))
    entry = make_entry(supplier_id=None)
    ok, errors = validator.validate(entry)
    assert not ok


def test_validator_passes():
    validator = EmissionValidator()
    validator.add_rule(EmissionRule("max_kg", 500))
    entry = make_entry(kg=100)
    ok, _ = validator.validate(entry)
    assert ok


# ─── Drift Detector ───────────────────────────────────────────────────────────

def test_drift_detector_no_drift():
    d = EmissionDriftDetector(threshold=0.15)
    d.record(1000.0)
    d.record(1010.0)
    assert not d.is_drifted()


def test_drift_detector_drifted():
    d = EmissionDriftDetector(threshold=0.10)
    d.record(1000.0)
    d.record(1200.0)
    assert d.is_drifted()


def test_drift_ratio():
    d = EmissionDriftDetector()
    d.record(1000.0)
    d.record(1500.0)
    assert abs(d.drift_ratio() - 0.5) < 1e-6


# ─── Exporter ─────────────────────────────────────────────────────────────────

def test_exporter_to_json():
    calc = EmissionCalculator()
    entry = calc.calculate("E1", EmissionScope.SCOPE1, "Gas", 100, "kwh", factor_key="kwh_natural_gas", period_start=P_START, period_end=P_END)
    report = calc.build_report("CORP", 2025, [entry])
    j = EmissionReportExporter.to_json(report)
    data = json.loads(j)
    assert "company_id" in data


def test_exporter_to_csv():
    calc = EmissionCalculator()
    entry = calc.calculate("E1", EmissionScope.SCOPE1, "Gas", 100, "kwh", factor_key="kwh_natural_gas", period_start=P_START, period_end=P_END)
    report = calc.build_report("CORP", 2025, [entry])
    csv = EmissionReportExporter.to_csv(report)
    assert "entry_id" in csv
    assert "E1" in csv


def test_exporter_to_markdown():
    calc = EmissionCalculator()
    entry = calc.calculate("E1", EmissionScope.SCOPE1, "Gas", 100, "kwh", factor_key="kwh_natural_gas", period_start=P_START, period_end=P_END)
    report = calc.build_report("CORP", 2025, [entry])
    md = EmissionReportExporter.to_markdown(report)
    assert "# Emission Report" in md


# ─── Diff ─────────────────────────────────────────────────────────────────────

def test_diff_entries():
    a = [make_entry("E1", kg=100), make_entry("E2", kg=200)]
    b = [make_entry("E1", kg=120), make_entry("E3", kg=50)]
    diff = diff_entries(a, b)
    assert "E3" in diff.added
    assert "E2" in diff.removed
    assert "E1" in diff.modified


def test_diff_summary():
    a = [make_entry("E1", kg=100)]
    b = [make_entry("E1", kg=200), make_entry("E2", kg=50)]
    diff = diff_entries(a, b)
    s = diff.summary()
    assert s["added"] == 1
    assert s["modified"] == 1


# ─── Streaming ────────────────────────────────────────────────────────────────

def test_stream_entries():
    entries = [make_entry(f"E{i}") for i in range(5)]
    result = list(stream_entries(entries))
    assert len(result) == 5


def test_entries_to_ndjson():
    entries = [make_entry("E1"), make_entry("E2")]
    lines = list(entries_to_ndjson(entries))
    assert len(lines) == 2
    assert lines[0].endswith("\n")


# ─── Rate Limiter ─────────────────────────────────────────────────────────────

def test_rate_limiter():
    limiter = RateLimiter(rate=100, capacity=5)
    for _ in range(5):
        assert limiter.acquire()
    assert not limiter.acquire()


# ─── Audit & PII ──────────────────────────────────────────────────────────────

def test_audit_log():
    log = AuditLog()
    log.record("calculated", "E001", detail="scope3 transport")
    entries = log.export()
    assert entries[0]["entry_id"] == "E001"


def test_pii_scrubber():
    result = PIIScrubber.scrub("Contact admin@example.com for access")
    assert "[EMAIL]" in result
    assert "admin@example.com" not in result
