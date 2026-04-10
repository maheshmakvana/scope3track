"""Advanced features for scope3track — caching, pipeline, async, observability, diff, security."""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar

from scope3track.models import EmissionEntry, EmissionReport, EmissionScope, Scope3Category

logger = logging.getLogger(__name__)
T = TypeVar("T")


# ─────────────────────────────────────────────────────────────────────────────
# CACHING
# ─────────────────────────────────────────────────────────────────────────────

class EmissionCache:
    """LRU + TTL cache for emission calculations, keyed by SHA-256."""

    def __init__(self, max_size: int = 512, ttl_seconds: float = 3600.0) -> None:
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def _key(self, *args: Any, **kwargs: Any) -> str:
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._store:
                self._misses += 1
                return None
            value, expires_at = self._store[key]
            if time.monotonic() > expires_at:
                del self._store[key]
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.monotonic() + self.ttl_seconds)
            while len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def memoize(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = self._key(fn.__name__, *args, **kwargs)
            cached = self.get(key)
            if cached is not None:
                return cached  # type: ignore[return-value]
            result = fn(*args, **kwargs)
            self.set(key, result)
            return result
        return wrapper

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "hits": self._hits, "misses": self._misses,
            "hit_rate": round(self._hits / total, 3) if total else 0.0,
            "size": len(self._store), "max_size": self.max_size, "ttl_seconds": self.ttl_seconds,
        }

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def save(self, path: str) -> None:
        import pickle
        with self._lock:
            with open(path, "wb") as f:
                pickle.dump(dict(self._store), f)

    def load(self, path: str) -> None:
        import pickle
        with open(path, "rb") as f:
            data = pickle.load(f)
        with self._lock:
            self._store = OrderedDict(data)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _Step:
    name: str
    fn: Callable
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)


class EmissionPipeline:
    """Fluent pipeline for chaining emission data transforms."""

    def __init__(self) -> None:
        self._steps: List[_Step] = []
        self._audit: List[Dict[str, Any]] = []
        self._retry_count = 0
        self._retry_delay = 0.5

    def map(self, fn: Callable[[List[EmissionEntry]], List[EmissionEntry]], name: str = "") -> "EmissionPipeline":
        self._steps.append(_Step(name=name or fn.__name__, fn=fn))
        return self

    def filter(self, predicate: Callable[[EmissionEntry], bool], name: str = "") -> "EmissionPipeline":
        def _f(entries: List[EmissionEntry]) -> List[EmissionEntry]:
            return [e for e in entries if predicate(e)]
        self._steps.append(_Step(name=name or "filter", fn=_f))
        return self

    def with_retry(self, count: int = 3, delay: float = 0.5) -> "EmissionPipeline":
        self._retry_count = count
        self._retry_delay = delay
        return self

    def run(self, entries: List[EmissionEntry]) -> List[EmissionEntry]:
        result = entries
        for step in self._steps:
            attempts = 0
            while True:
                try:
                    t0 = time.monotonic()
                    result = step.fn(result)
                    self._audit.append({"step": step.name, "in": len(entries), "out": len(result), "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "ok": True})
                    break
                except Exception as exc:
                    attempts += 1
                    if attempts > self._retry_count:
                        self._audit.append({"step": step.name, "error": str(exc), "ok": False})
                        raise
                    time.sleep(self._retry_delay)
        return result

    async def arun(self, entries: List[EmissionEntry]) -> List[EmissionEntry]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.run(entries))

    def audit_log(self) -> List[Dict[str, Any]]:
        return list(self._audit)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EmissionRule:
    rule_type: str  # "max_kg", "required_supplier", "scope_required", "positive_factor"
    value: Any
    message: str = ""


class EmissionValidator:
    """Declarative rule-based validator for emission entries."""

    def __init__(self) -> None:
        self._rules: List[EmissionRule] = []

    def add_rule(self, rule: EmissionRule) -> "EmissionValidator":
        self._rules.append(rule)
        return self

    def validate(self, entry: EmissionEntry) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        for rule in self._rules:
            if rule.rule_type == "max_kg" and entry.emissions_kg_co2e > rule.value:
                errors.append(rule.message or f"Entry {entry.entry_id}: emissions {entry.emissions_kg_co2e} exceeds max {rule.value} kg CO2e")
            elif rule.rule_type == "required_supplier" and not entry.supplier_id:
                errors.append(rule.message or f"Entry {entry.entry_id}: missing supplier_id")
            elif rule.rule_type == "positive_factor" and entry.emission_factor <= 0:
                errors.append(rule.message or f"Entry {entry.entry_id}: emission_factor must be > 0")
        return len(errors) == 0, errors

    def validate_batch(self, entries: List[EmissionEntry]) -> Dict[str, List[str]]:
        return {e.entry_id: self.validate(e)[1] for e in entries if not self.validate(e)[0]}


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC & CONCURRENCY
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def async_acquire(self, tokens: float = 1.0) -> bool:
        while not self.acquire(tokens):
            await asyncio.sleep(0.05)
        return True


class CancellationToken:
    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled


def batch_calculate(
    entries: List[Dict[str, Any]],
    calc_fn: Callable[[Dict[str, Any]], EmissionEntry],
    max_workers: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[EmissionEntry]:
    results: List[EmissionEntry] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(calc_fn, e): e for e in entries}
        for future in as_completed(futures):
            if token and token.is_cancelled:
                break
            results.append(future.result())
    return results


async def abatch_calculate(
    entries: List[Dict[str, Any]],
    calc_fn: Callable[[Dict[str, Any]], EmissionEntry],
    max_concurrency: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[EmissionEntry]:
    sem = asyncio.Semaphore(max_concurrency)
    loop = asyncio.get_event_loop()

    async def run_one(e: Dict[str, Any]) -> EmissionEntry:
        async with sem:
            if token and token.is_cancelled:
                raise asyncio.CancelledError()
            return await loop.run_in_executor(None, lambda: calc_fn(e))

    return list(await asyncio.gather(*[run_one(e) for e in entries]))


# ─────────────────────────────────────────────────────────────────────────────
# OBSERVABILITY
# ─────────────────────────────────────────────────────────────────────────────

class EmissionProfiler:
    def __init__(self) -> None:
        self._records: List[Dict[str, Any]] = []

    def profile(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            t0 = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                self._records.append({"fn": fn.__name__, "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "ok": True})
                return result
            except Exception as exc:
                self._records.append({"fn": fn.__name__, "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc), "ok": False})
                raise
        return wrapper

    def report(self) -> List[Dict[str, Any]]:
        return list(self._records)


class EmissionDriftDetector:
    """Detect drift in emission totals across reporting periods."""

    def __init__(self, threshold: float = 0.15) -> None:
        self.threshold = threshold
        self._history: List[float] = []

    def record(self, total_kg_co2e: float) -> None:
        self._history.append(total_kg_co2e)

    def is_drifted(self) -> bool:
        if len(self._history) < 2:
            return False
        prev, latest = self._history[-2], self._history[-1]
        if prev == 0:
            return False
        return abs(latest - prev) / prev > self.threshold

    def drift_ratio(self) -> float:
        if len(self._history) < 2 or self._history[-2] == 0:
            return 0.0
        prev, latest = self._history[-2], self._history[-1]
        return (latest - prev) / prev


class EmissionReportExporter:
    """Export EmissionReport to JSON, CSV, Markdown."""

    @staticmethod
    def to_json(report: EmissionReport) -> str:
        return json.dumps(report.summary(), indent=2)

    @staticmethod
    def to_csv(report: EmissionReport) -> str:
        lines = ["entry_id,scope,category,source,activity_amount,activity_unit,emission_factor,emissions_kg_co2e"]
        for e in report.entries:
            lines.append(f"{e.entry_id},{e.scope.value},{e.category.value if e.category else ''},{e.source},{e.activity_amount},{e.activity_unit},{e.emission_factor},{e.emissions_kg_co2e}")
        return "\n".join(lines)

    @staticmethod
    def to_markdown(report: EmissionReport) -> str:
        s = report.summary()
        lines = [f"# Emission Report — {report.company_id} ({report.reporting_year})", ""]
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        for k, v in s.items():
            lines.append(f"| {k} | {v} |")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING
# ─────────────────────────────────────────────────────────────────────────────

def stream_entries(entries: List[EmissionEntry]) -> Generator[EmissionEntry, None, None]:
    for e in entries:
        yield e


def entries_to_ndjson(entries: List[EmissionEntry]) -> Generator[str, None, None]:
    for e in entries:
        yield e.model_dump_json() + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# DIFF
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EmissionDiff:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    modified: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def summary(self) -> Dict[str, Any]:
        return {"added": len(self.added), "removed": len(self.removed), "modified": len(self.modified)}

    def to_json(self) -> str:
        return json.dumps({"added": self.added, "removed": self.removed, "modified": self.modified})


def diff_entries(a: List[EmissionEntry], b: List[EmissionEntry]) -> EmissionDiff:
    map_a = {e.entry_id: e for e in a}
    map_b = {e.entry_id: e for e in b}
    diff = EmissionDiff(
        added=[eid for eid in map_b if eid not in map_a],
        removed=[eid for eid in map_a if eid not in map_b],
    )
    for eid in set(map_a) & set(map_b):
        changes: Dict[str, Any] = {}
        for f in ("emissions_kg_co2e", "emission_factor", "activity_amount"):
            va, vb = getattr(map_a[eid], f), getattr(map_b[eid], f)
            if abs(va - vb) > 1e-9:
                changes[f] = {"old": va, "new": vb}
        if changes:
            diff.modified[eid] = changes
    return diff


# ─────────────────────────────────────────────────────────────────────────────
# SECURITY
# ─────────────────────────────────────────────────────────────────────────────

class AuditLog:
    def __init__(self) -> None:
        self._entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(self, action: str, entry_id: str, detail: Optional[str] = None) -> None:
        with self._lock:
            self._entries.append({"ts": datetime.utcnow().isoformat(), "action": action, "entry_id": entry_id, "detail": detail})

    def export(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)


class PIIScrubber:
    import re as _re
    _EMAIL = _re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

    @classmethod
    def scrub(cls, text: str) -> str:
        return cls._EMAIL.sub("[EMAIL]", text)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: SCIENCE-BASED TARGETS (SBTi) ALIGNMENT CHECKER
# ─────────────────────────────────────────────────────────────────────────────

# GHG Protocol standard reduction trajectories (% reduction per year from base year)
_SBTI_PATHWAYS: Dict[str, Dict[str, Any]] = {
    "1.5C":  {"annual_reduction_pct": 0.042, "description": "1.5°C pathway — 4.2% absolute reduction per year"},
    "well_below_2C": {"annual_reduction_pct": 0.025, "description": "Well-below 2°C pathway — 2.5% absolute reduction per year"},
    "2C":    {"annual_reduction_pct": 0.018, "description": "2°C pathway — 1.8% absolute reduction per year"},
}

_SCOPE3_MATERIAL_CATEGORIES: List[str] = [
    "purchased_goods_services",
    "capital_goods",
    "use_of_sold_products",
    "end_of_life_treatment",
    "investments",
]


@dataclass
class SBTiAlignmentResult:
    """SBTi alignment assessment for a company's emission report."""
    company_id: str
    reporting_year: int
    pathway: str
    base_year: int
    base_year_total_t_co2e: float
    current_total_t_co2e: float
    required_total_t_co2e: float
    years_elapsed: int
    on_track: bool
    gap_t_co2e: float
    scope3_coverage_pct: float
    scope3_material: bool
    recommendations: List[str]

    def summary(self) -> Dict[str, Any]:
        return {
            "company_id": self.company_id,
            "reporting_year": self.reporting_year,
            "pathway": self.pathway,
            "on_track": self.on_track,
            "current_t_co2e": round(self.current_total_t_co2e, 2),
            "required_t_co2e": round(self.required_total_t_co2e, 2),
            "gap_t_co2e": round(self.gap_t_co2e, 2),
            "scope3_material": self.scope3_material,
            "scope3_coverage_pct": round(self.scope3_coverage_pct, 1),
        }


class SBTiAlignmentChecker:
    """
    Assess whether a company's emissions trajectory aligns with SBTi pathways.

    Computes the required total emissions for the current year based on a
    base-year snapshot and the chosen decarbonization pathway (1.5°C,
    well-below 2°C, or 2°C). Flags whether Scope 3 is material (>40% of total)
    and therefore required under SBTi rules.

    Usage::

        checker = SBTiAlignmentChecker()
        result = checker.check(
            report=current_report,
            base_year=2020,
            base_year_total_t_co2e=12000.0,
            pathway="1.5C",
        )
        print(checker.to_markdown(result))
    """

    PATHWAYS = list(_SBTI_PATHWAYS.keys())

    def check(
        self,
        report: EmissionReport,
        base_year: int,
        base_year_total_t_co2e: float,
        pathway: str = "1.5C",
    ) -> SBTiAlignmentResult:
        """Perform SBTi alignment check against a baseline."""
        if pathway not in _SBTI_PATHWAYS:
            raise ValueError(f"Unknown pathway '{pathway}'. Choose from {self.PATHWAYS}")

        rate = _SBTI_PATHWAYS[pathway]["annual_reduction_pct"]
        years = max(0, report.reporting_year - base_year)
        required = base_year_total_t_co2e * ((1 - rate) ** years)
        current = report.total_t_co2e
        gap = current - required

        # Scope 3 materiality: >40% of total means Scope 3 target is required
        scope3_pct = (report.scope3_kg_co2e / report.total_kg_co2e * 100) if report.total_kg_co2e > 0 else 0.0
        scope3_material = scope3_pct >= 40.0

        # Scope 3 category coverage
        covered_cats = set()
        for e in report.entries:
            if e.scope == EmissionScope.SCOPE3 and e.category:
                covered_cats.add(e.category.value)
        coverage_pct = (len(covered_cats) / 15 * 100) if covered_cats else 0.0

        recs: List[str] = []
        if not report.on_track if hasattr(report, "on_track") else gap > 0:
            recs.append(f"Reduce total emissions by {round(gap, 1)} t CO2e to align with {pathway} pathway.")
        if scope3_material and coverage_pct < 67:
            recs.append("SBTi requires Scope 3 targets when material. Expand Scope 3 data collection to cover ≥2/3 of categories.")
        if report.scope1_kg_co2e / 1000 > required * 0.5:
            recs.append("Scope 1 represents >50% of target budget. Prioritise operational decarbonisation (fuel switching, electrification).")
        if not recs:
            recs.append(f"On track for {pathway} pathway. Maintain reduction momentum.")

        return SBTiAlignmentResult(
            company_id=report.company_id,
            reporting_year=report.reporting_year,
            pathway=pathway,
            base_year=base_year,
            base_year_total_t_co2e=base_year_total_t_co2e,
            current_total_t_co2e=current,
            required_total_t_co2e=required,
            years_elapsed=years,
            on_track=gap <= 0,
            gap_t_co2e=gap,
            scope3_coverage_pct=coverage_pct,
            scope3_material=scope3_material,
            recommendations=recs,
        )

    def multi_pathway_comparison(
        self,
        report: EmissionReport,
        base_year: int,
        base_year_total_t_co2e: float,
    ) -> List[Dict[str, Any]]:
        """Compare alignment across all three SBTi pathways."""
        return [self.check(report, base_year, base_year_total_t_co2e, pw).summary() for pw in self.PATHWAYS]

    def to_markdown(self, result: SBTiAlignmentResult) -> str:
        """Render a Markdown SBTi alignment report."""
        status = "ON TRACK" if result.on_track else "OFF TRACK"
        lines = [
            f"# SBTi Alignment Report — {result.company_id} ({result.reporting_year})",
            f"**Pathway**: {result.pathway}  |  **Status**: {status}",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Base Year | {result.base_year} |",
            f"| Base Year Total | {result.base_year_total_t_co2e:.1f} t CO2e |",
            f"| Current Total | {result.current_total_t_co2e:.2f} t CO2e |",
            f"| Required Total | {result.required_total_t_co2e:.2f} t CO2e |",
            f"| Gap | {result.gap_t_co2e:+.2f} t CO2e |",
            f"| Scope 3 Material | {'Yes (target required)' if result.scope3_material else 'No'} |",
            f"| Scope 3 Coverage | {result.scope3_coverage_pct:.1f}% of GHG Protocol categories |",
            "",
            "## Recommendations",
        ]
        for rec in result.recommendations:
            lines.append(f"- {rec}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: SUPPLIER EMISSION RANKER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SupplierRank:
    """Supplier emission rank with intensity and engagement priority."""
    rank: int
    supplier_id: str
    total_kg_co2e: float
    entry_count: int
    avg_emission_intensity: float   # kg CO2e per activity unit
    verified: bool
    share_of_total_pct: float
    engagement_priority: str        # "critical", "high", "medium", "low"
    action: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rank": self.rank,
            "supplier_id": self.supplier_id,
            "total_kg_co2e": round(self.total_kg_co2e, 3),
            "entry_count": self.entry_count,
            "avg_emission_intensity": round(self.avg_emission_intensity, 6),
            "verified": self.verified,
            "share_of_total_pct": round(self.share_of_total_pct, 2),
            "engagement_priority": self.engagement_priority,
            "action": self.action,
        }


class SupplierEmissionRanker:
    """
    Rank Scope 3 suppliers by emission contribution and engagement priority.

    Implements a Pareto-style analysis: identifies the top suppliers that
    account for the largest share of Scope 3 emissions, flags unverified
    suppliers, and assigns data collection engagement priorities.

    Usage::

        ranker = SupplierEmissionRanker()
        rankings = ranker.rank(report)
        print(ranker.to_markdown(rankings))
    """

    def rank(self, report: EmissionReport) -> List[SupplierRank]:
        """Rank all suppliers in the report by total Scope 3 contribution."""
        if not report.suppliers:
            # Fall back to aggregating entries by supplier_id field
            return self._rank_from_entries(report)

        total_kg = report.scope3_kg_co2e or 1.0
        ranked = []
        sorted_suppliers = sorted(report.suppliers, key=lambda s: s.total_kg_co2e, reverse=True)

        for i, supplier in enumerate(sorted_suppliers, 1):
            share = supplier.total_kg_co2e / total_kg * 100
            avg_intensity = 0.0
            if supplier.entries:
                intensities = [
                    e.emissions_kg_co2e / e.activity_amount
                    for e in supplier.entries if e.activity_amount > 0
                ]
                avg_intensity = sum(intensities) / len(intensities) if intensities else 0.0

            if share >= 20:
                priority, action = "critical", "Mandate verified primary data submission within 60 days."
            elif share >= 10:
                priority, action = "high", "Request carbon footprint disclosure and reduction roadmap."
            elif share >= 5:
                priority, action = "medium", "Include in annual Scope 3 data collection campaign."
            else:
                priority, action = "low", "Use spend-based estimation; revisit if spend increases."

            if not supplier.verified:
                priority = "critical" if priority in ("low", "medium") else priority
                action = "Unverified data — prioritise verification before reporting. " + action

            ranked.append(SupplierRank(
                rank=i,
                supplier_id=supplier.supplier_id,
                total_kg_co2e=supplier.total_kg_co2e,
                entry_count=len(supplier.entries),
                avg_emission_intensity=avg_intensity,
                verified=supplier.verified,
                share_of_total_pct=share,
                engagement_priority=priority,
                action=action,
            ))
        return ranked

    def _rank_from_entries(self, report: EmissionReport) -> List[SupplierRank]:
        """Aggregate Scope 3 entries by supplier_id when no SupplierEmissions objects present."""
        supplier_totals: Dict[str, float] = {}
        supplier_entries: Dict[str, List[EmissionEntry]] = {}
        for e in report.entries:
            if e.scope == EmissionScope.SCOPE3 and e.supplier_id:
                supplier_totals[e.supplier_id] = supplier_totals.get(e.supplier_id, 0.0) + e.emissions_kg_co2e
                supplier_entries.setdefault(e.supplier_id, []).append(e)

        total = sum(supplier_totals.values()) or 1.0
        ranked = []
        for i, (sid, total_kg) in enumerate(sorted(supplier_totals.items(), key=lambda x: x[1], reverse=True), 1):
            entries = supplier_entries[sid]
            intensities = [e.emissions_kg_co2e / e.activity_amount for e in entries if e.activity_amount > 0]
            avg_i = sum(intensities) / len(intensities) if intensities else 0.0
            share = total_kg / total * 100
            priority = "critical" if share >= 20 else "high" if share >= 10 else "medium" if share >= 5 else "low"
            action = "Engage for primary data." if share >= 10 else "Use estimation."
            ranked.append(SupplierRank(i, sid, total_kg, len(entries), avg_i, False, share, priority, action))
        return ranked

    def top_n(self, report: EmissionReport, n: int = 10) -> List[SupplierRank]:
        """Return the top-N emitting suppliers."""
        return self.rank(report)[:n]

    def cumulative_coverage(self, rankings: List[SupplierRank], target_pct: float = 80.0) -> List[SupplierRank]:
        """Return the minimal set of suppliers covering target_pct% of Scope 3 emissions."""
        cumulative = 0.0
        result = []
        for r in rankings:
            result.append(r)
            cumulative += r.share_of_total_pct
            if cumulative >= target_pct:
                break
        return result

    def to_markdown(self, rankings: List[SupplierRank]) -> str:
        """Render a Markdown supplier ranking table."""
        lines = ["# Supplier Emission Ranking", "",
                 "| Rank | Supplier ID | Total t CO2e | Share % | Verified | Priority | Action |",
                 "|------|-------------|-------------|---------|----------|----------|--------|"]
        for r in rankings:
            lines.append(
                f"| {r.rank} | {r.supplier_id} | {r.total_kg_co2e / 1000:.2f} | "
                f"{r.share_of_total_pct:.1f}% | {'YES' if r.verified else 'NO'} | "
                f"{r.engagement_priority.upper()} | {r.action[:60]}… |"
            )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: CARBON REDUCTION SCENARIO MODELLER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReductionScenario:
    """A what-if reduction intervention to model."""
    name: str
    scope: str          # "scope1", "scope2", "scope3", "all"
    category: Optional[str]  # Scope3Category value, or None for all
    reduction_pct: float     # 0.0 to 1.0
    cost_estimate_usd: Optional[float] = None
    description: str = ""


@dataclass
class ScenarioModelResult:
    """Projected outcome of applying a ReductionScenario."""
    scenario_name: str
    baseline_kg_co2e: float
    projected_kg_co2e: float
    absolute_reduction_kg: float
    reduction_pct_achieved: float
    cost_estimate_usd: Optional[float]
    cost_per_tonne_co2e: Optional[float]
    description: str

    def summary(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario_name,
            "baseline_t_co2e": round(self.baseline_kg_co2e / 1000, 3),
            "projected_t_co2e": round(self.projected_kg_co2e / 1000, 3),
            "reduction_t_co2e": round(self.absolute_reduction_kg / 1000, 3),
            "reduction_pct": round(self.reduction_pct_achieved * 100, 2),
            "cost_usd": self.cost_estimate_usd,
            "cost_per_tonne": round(self.cost_per_tonne_co2e, 2) if self.cost_per_tonne_co2e else None,
        }


class CarbonReductionScenarioModeller:
    """
    Model what-if carbon reduction scenarios against an emission report.

    Applies percentage-based reductions to specified scopes and/or Scope 3
    categories, calculates the projected total emissions, and estimates
    cost-efficiency ($/t CO2e) when cost data is provided.

    Usage::

        modeller = CarbonReductionScenarioModeller()
        scenario = ReductionScenario(
            name="Switch to renewable electricity",
            scope="scope2",
            category=None,
            reduction_pct=1.0,
            cost_estimate_usd=50000,
        )
        result = modeller.model(report, scenario)
        print(result.summary())
    """

    def model(self, report: EmissionReport, scenario: ReductionScenario) -> ScenarioModelResult:
        """Apply one reduction scenario to an EmissionReport."""
        baseline = report.total_kg_co2e
        reduction_kg = self._compute_reduction(report, scenario)
        projected = max(0.0, baseline - reduction_kg)
        pct_achieved = reduction_kg / baseline if baseline > 0 else 0.0

        cost_per_tonne: Optional[float] = None
        if scenario.cost_estimate_usd and reduction_kg > 0:
            cost_per_tonne = scenario.cost_estimate_usd / (reduction_kg / 1000)

        return ScenarioModelResult(
            scenario_name=scenario.name,
            baseline_kg_co2e=baseline,
            projected_kg_co2e=projected,
            absolute_reduction_kg=reduction_kg,
            reduction_pct_achieved=pct_achieved,
            cost_estimate_usd=scenario.cost_estimate_usd,
            cost_per_tonne_co2e=cost_per_tonne,
            description=scenario.description,
        )

    def _compute_reduction(self, report: EmissionReport, scenario: ReductionScenario) -> float:
        """Compute absolute reduction in kg CO2e for a scenario."""
        scope = scenario.scope
        category = scenario.category
        pct = scenario.reduction_pct

        if scope == "scope1":
            return report.scope1_kg_co2e * pct
        if scope == "scope2":
            return report.scope2_kg_co2e * pct
        if scope == "scope3":
            if category:
                cat_total = sum(
                    e.emissions_kg_co2e for e in report.entries
                    if e.scope == EmissionScope.SCOPE3 and e.category and e.category.value == category
                )
                return cat_total * pct
            return report.scope3_kg_co2e * pct
        # "all"
        return report.total_kg_co2e * pct

    def compare_scenarios(
        self,
        report: EmissionReport,
        scenarios: List[ReductionScenario],
    ) -> List[Dict[str, Any]]:
        """Run multiple scenarios and return a sorted comparison (best reduction first)."""
        results = [self.model(report, s) for s in scenarios]
        return sorted([r.summary() for r in results], key=lambda x: x["reduction_t_co2e"], reverse=True)

    def to_markdown(self, results: List[Dict[str, Any]]) -> str:
        """Render scenario comparison as Markdown."""
        lines = ["# Carbon Reduction Scenario Modelling", "",
                 "| Scenario | Baseline t | Projected t | Reduction t | Reduction % | $/t CO2e |",
                 "|----------|-----------|------------|------------|-------------|---------|"]
        for r in results:
            cpt = f"${r['cost_per_tonne']:.2f}" if r["cost_per_tonne"] else "—"
            lines.append(
                f"| {r['scenario']} | {r['baseline_t_co2e']:.1f} | {r['projected_t_co2e']:.1f} | "
                f"{r['reduction_t_co2e']:.3f} | {r['reduction_pct']:.1f}% | {cpt} |"
            )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: EMISSION SPAN EMITTER (OpenTelemetry with stdlib fallback)
# ─────────────────────────────────────────────────────────────────────────────

class EmissionSpanEmitter:
    """
    Emit OpenTelemetry spans for emission calculation operations.
    Falls back to structured logging when opentelemetry-sdk is not installed.
    """

    def __init__(self, service_name: str = "scope3track") -> None:
        self._service = service_name
        self._otel_available = False
        self._tracer: Any = None
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            provider = TracerProvider()
            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(service_name)
            self._otel_available = True
            logger.debug("EmissionSpanEmitter: OpenTelemetry tracer initialised")
        except ImportError:
            logger.debug("EmissionSpanEmitter: opentelemetry not installed — using log fallback")

    def span(self, operation: str, attributes: Optional[Dict[str, Any]] = None) -> Any:
        """Context manager: emit an OTEL span or log span start/end."""
        if self._otel_available and self._tracer is not None:
            span = self._tracer.start_span(operation)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, str(v))
            return span
        return _LogSpan(operation, attributes or {}, self._service)

    def emit_entry(self, entry: EmissionEntry) -> None:
        """Emit a span for a single emission entry calculation."""
        attrs = {
            "entry_id": entry.entry_id,
            "scope": entry.scope.value,
            "emissions_kg_co2e": entry.emissions_kg_co2e,
            "source": entry.source,
        }
        with self.span("scope3track.emission_calculated", attrs):
            pass

    def emit_report(self, report: EmissionReport) -> None:
        """Emit a span summarising a full emission report."""
        attrs = {
            "company_id": report.company_id,
            "reporting_year": report.reporting_year,
            "total_t_co2e": report.total_t_co2e,
            "entry_count": len(report.entries),
        }
        with self.span("scope3track.report_generated", attrs):
            pass


class _LogSpan:
    """Stdlib-logging fallback span used when OTEL is unavailable."""

    def __init__(self, name: str, attrs: Dict[str, Any], service: str) -> None:
        self._name = name
        self._attrs = attrs
        self._service = service
        self._t0 = time.monotonic()

    def __enter__(self) -> "_LogSpan":
        logger.debug("[span:start] service=%s operation=%s attrs=%s", self._service, self._name, self._attrs)
        return self

    def __exit__(self, *args: Any) -> None:
        elapsed = round((time.monotonic() - self._t0) * 1000, 2)
        logger.debug("[span:end] service=%s operation=%s elapsed_ms=%s", self._service, self._name, elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT v1.2.0: EMISSION HOTSPOT ANALYZER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EmissionHotspot:
    """A high-emission source identified from an EmissionReport."""
    rank: int
    source: str
    category: Optional[str]   # Scope3Category value or None
    scope: str
    total_kg_co2e: float
    share_pct: float
    cumulative_pct: float
    entry_count: int
    avg_intensity: float      # kg CO2e per unit of activity
    priority: str             # "critical", "high", "medium", "low"
    action: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rank": self.rank,
            "source": self.source,
            "scope": self.scope,
            "category": self.category,
            "total_t_co2e": round(self.total_kg_co2e / 1000, 3),
            "share_pct": round(self.share_pct, 2),
            "priority": self.priority,
            "action": self.action,
        }


class EmissionHotspotAnalyzer:
    """
    Identify the highest-emission sources across scopes and Scope 3 categories.

    Aggregates EmissionEntries by source description, computes each source's
    share of total portfolio emissions, and classifies them by abatement priority.
    Supports Pareto (80/20) filtering to focus reduction efforts on the largest
    contributors first.

    Usage::

        analyzer = EmissionHotspotAnalyzer()
        hotspots = analyzer.analyze(report)
        print(analyzer.to_markdown(hotspots))
        top80 = analyzer.pareto(hotspots)   # sources covering 80% of emissions
    """

    _PRIORITY_THRESHOLDS = (20.0, 10.0, 5.0)  # % share → critical / high / medium

    _SCOPE_ACTIONS: Dict[str, str] = {
        "scope1": "Fuel-switch to renewable energy or electrification; retrofit combustion equipment.",
        "scope2": "Procure 100% renewable electricity certificates (RECs/GOs); install on-site solar.",
        "scope3": "Engage supplier for primary emission data; set joint reduction target.",
    }

    def analyze(self, report: EmissionReport) -> List[EmissionHotspot]:
        """Aggregate entries by source and return ranked hotspots."""
        from collections import defaultdict
        source_kg: Dict[str, float] = defaultdict(float)
        source_entries: Dict[str, List[EmissionEntry]] = defaultdict(list)

        for entry in report.entries:
            source_kg[entry.source] += entry.emissions_kg_co2e
            source_entries[entry.source].append(entry)

        total = sum(source_kg.values()) or 1.0
        sorted_sources = sorted(source_kg.items(), key=lambda x: x[1], reverse=True)
        cumulative = 0.0
        hotspots: List[EmissionHotspot] = []

        for rank, (source, kg) in enumerate(sorted_sources, 1):
            share = kg / total * 100
            cumulative += share
            entries = source_entries[source]

            scope = entries[0].scope.value if entries else "scope3"
            category = entries[0].scope3_category.value if entries and entries[0].scope3_category else None
            intensities = [e.emissions_kg_co2e / e.activity_amount for e in entries if e.activity_amount > 0]
            avg_i = sum(intensities) / len(intensities) if intensities else 0.0

            if share >= self._PRIORITY_THRESHOLDS[0]:
                priority = "critical"
            elif share >= self._PRIORITY_THRESHOLDS[1]:
                priority = "high"
            elif share >= self._PRIORITY_THRESHOLDS[2]:
                priority = "medium"
            else:
                priority = "low"

            action = self._SCOPE_ACTIONS.get(scope, "Review and reduce through efficiency measures.")
            hotspots.append(EmissionHotspot(
                rank=rank,
                source=source,
                category=category,
                scope=scope,
                total_kg_co2e=kg,
                share_pct=share,
                cumulative_pct=cumulative,
                entry_count=len(entries),
                avg_intensity=avg_i,
                priority=priority,
                action=action,
            ))
        return hotspots

    def pareto(self, hotspots: List[EmissionHotspot], target_pct: float = 80.0) -> List[EmissionHotspot]:
        """Return the minimal set of sources covering target_pct of total emissions."""
        result: List[EmissionHotspot] = []
        cumulative = 0.0
        for h in hotspots:
            result.append(h)
            cumulative += h.share_pct
            if cumulative >= target_pct:
                break
        return result

    def by_scope(self, hotspots: List[EmissionHotspot], scope: str) -> List[EmissionHotspot]:
        """Filter hotspots to a specific scope ('scope1', 'scope2', 'scope3')."""
        return [h for h in hotspots if h.scope == scope]

    def to_markdown(self, hotspots: List[EmissionHotspot]) -> str:
        """Render a Markdown emission hotspot table."""
        lines = [
            "# Emission Hotspot Analysis",
            "",
            "| Rank | Source | Scope | t CO2e | Share % | Cumul. % | Priority | Action |",
            "|------|--------|-------|--------|---------|----------|----------|--------|",
        ]
        for h in hotspots:
            lines.append(
                f"| {h.rank} | {h.source[:35]} | {h.scope} | "
                f"{h.total_kg_co2e / 1000:.2f} | {h.share_pct:.1f}% | "
                f"{h.cumulative_pct:.1f}% | {h.priority.upper()} | "
                f"{h.action[:50]}… |"
            )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT v1.2.0: NET ZERO ROADMAP GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NetZeroMilestone:
    """A single year milestone on the path to Net Zero."""
    year: int
    target_kg_co2e: float
    required_reduction_from_base_kg: float
    cumulative_reduction_pct: float
    key_actions: List[str]
    offset_needed_kg: float   # residual emissions to offset if abatement alone is insufficient

    def to_dict(self) -> Dict[str, Any]:
        return {
            "year": self.year,
            "target_t_co2e": round(self.target_kg_co2e / 1000, 2),
            "required_reduction_t": round(self.required_reduction_from_base_kg / 1000, 2),
            "cumulative_reduction_pct": round(self.cumulative_reduction_pct, 1),
            "key_actions": self.key_actions,
            "offset_needed_t": round(self.offset_needed_kg / 1000, 2),
        }


@dataclass
class NetZeroRoadmap:
    """A complete year-by-year Net Zero roadmap for an organisation."""
    company_id: str
    base_year: int
    target_year: int
    base_kg_co2e: float
    pathway: str                # "1.5C", "well_below_2C", "2C"
    milestones: List[NetZeroMilestone]
    total_abatement_needed_kg: float
    feasibility: str            # "achievable", "challenging", "requires_offsets"
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def summary(self) -> Dict[str, Any]:
        return {
            "company_id": self.company_id,
            "pathway": self.pathway,
            "base_year": self.base_year,
            "target_year": self.target_year,
            "base_t_co2e": round(self.base_kg_co2e / 1000, 2),
            "total_abatement_needed_t": round(self.total_abatement_needed_kg / 1000, 2),
            "feasibility": self.feasibility,
            "milestone_count": len(self.milestones),
        }


# Typical corporate abatement lever menu, phase-sorted by typical deployment timeline
_ABATEMENT_LEVERS: List[Dict[str, Any]] = [
    {"phase": "short",  "action": "Switch to 100% renewable electricity (PPAs/RECs).",           "scope": "scope2"},
    {"phase": "short",  "action": "Electrify light-duty fleet (replace ICE with EVs).",           "scope": "scope1"},
    {"phase": "short",  "action": "Implement remote-work / business travel reduction policy.",    "scope": "scope3"},
    {"phase": "medium", "action": "Retrofit building HVAC with heat pumps and insulation.",       "scope": "scope1"},
    {"phase": "medium", "action": "Switch manufacturing fuel to green hydrogen or biomethane.",   "scope": "scope1"},
    {"phase": "medium", "action": "Require top-20 suppliers to set Science-Based Targets.",       "scope": "scope3"},
    {"phase": "medium", "action": "Redesign products for circularity; reduce packaging weight.",  "scope": "scope3"},
    {"phase": "long",   "action": "Deploy CCS / CCUS at high-emission industrial processes.",     "scope": "scope1"},
    {"phase": "long",   "action": "Achieve full supply chain Scope 3 primary data coverage.",     "scope": "scope3"},
    {"phase": "long",   "action": "Purchase verified carbon offsets for residual hard-to-abate.", "scope": "all"},
]


class NetZeroRoadmapGenerator:
    """
    Generate a year-by-year Net Zero roadmap aligned to an SBTi pathway.

    Distributes the required annual reduction trajectory across milestone years
    and assigns phased abatement levers (short / medium / long term). Identifies
    residual emissions that must be offset when abatement alone cannot reach zero.

    Usage::

        generator = NetZeroRoadmapGenerator()
        roadmap = generator.generate(
            company_id="acme-corp",
            base_year=2023,
            target_year=2050,
            base_kg_co2e=500_000,
            pathway="1.5C",
            milestone_years=[2025, 2030, 2035, 2040, 2045, 2050],
        )
        print(generator.to_markdown(roadmap))
    """

    _PATHWAY_RATES: Dict[str, float] = {
        "1.5C":          0.042,
        "well_below_2C": 0.025,
        "2C":            0.018,
    }

    def generate(
        self,
        company_id: str,
        base_year: int,
        target_year: int,
        base_kg_co2e: float,
        pathway: str = "1.5C",
        milestone_years: Optional[List[int]] = None,
    ) -> NetZeroRoadmap:
        """Generate a Net Zero roadmap for the given parameters."""
        rate = self._PATHWAY_RATES.get(pathway, 0.042)
        if milestone_years is None:
            step = max(1, (target_year - base_year) // 5)
            milestone_years = list(range(base_year + step, target_year + 1, step))
            if target_year not in milestone_years:
                milestone_years.append(target_year)

        milestones: List[NetZeroMilestone] = []
        for year in sorted(milestone_years):
            years_elapsed = year - base_year
            # Compound reduction: target_kg = base * (1 - rate)^years
            compound_factor = (1.0 - rate) ** years_elapsed
            target_kg = base_kg_co2e * compound_factor
            reduction_from_base = base_kg_co2e - target_kg
            cumulative_pct = (1.0 - compound_factor) * 100

            # Assign levers by phase
            phase = "short" if years_elapsed <= 5 else "medium" if years_elapsed <= 15 else "long"
            actions = [
                lev["action"] for lev in _ABATEMENT_LEVERS
                if lev["phase"] == phase
            ][:3]

            # Residual offset needed when approaching target year
            offset_kg = max(0.0, target_kg * 0.05) if year == target_year else 0.0

            milestones.append(NetZeroMilestone(
                year=year,
                target_kg_co2e=target_kg,
                required_reduction_from_base_kg=reduction_from_base,
                cumulative_reduction_pct=cumulative_pct,
                key_actions=actions,
                offset_needed_kg=offset_kg,
            ))

        total_abatement = base_kg_co2e - milestones[-1].target_kg_co2e if milestones else 0.0
        residual = milestones[-1].target_kg_co2e if milestones else base_kg_co2e
        if residual < base_kg_co2e * 0.10:
            feasibility = "achievable"
        elif residual < base_kg_co2e * 0.25:
            feasibility = "challenging"
        else:
            feasibility = "requires_offsets"

        return NetZeroRoadmap(
            company_id=company_id,
            base_year=base_year,
            target_year=target_year,
            base_kg_co2e=base_kg_co2e,
            pathway=pathway,
            milestones=milestones,
            total_abatement_needed_kg=total_abatement,
            feasibility=feasibility,
        )

    def to_markdown(self, roadmap: NetZeroRoadmap) -> str:
        """Render a full Markdown Net Zero roadmap report."""
        s = roadmap.summary()
        lines = [
            f"# Net Zero Roadmap — {roadmap.company_id}",
            f"**Pathway**: {roadmap.pathway}  |  "
            f"**Base Year**: {roadmap.base_year} ({s['base_t_co2e']:.0f} t CO2e)  |  "
            f"**Target Year**: {roadmap.target_year}  |  "
            f"**Feasibility**: {roadmap.feasibility.replace('_', ' ').upper()}",
            "",
            "## Year-by-Year Milestones",
            "",
            "| Year | Target t CO2e | Reduction from Base | Cumul. % | Offset Needed t |",
            "|------|--------------|---------------------|----------|-----------------|",
        ]
        for m in roadmap.milestones:
            lines.append(
                f"| {m.year} | {m.target_kg_co2e / 1000:.1f} | "
                f"{m.required_reduction_from_base_kg / 1000:.1f} t | "
                f"{m.cumulative_reduction_pct:.1f}% | "
                f"{m.offset_needed_kg / 1000:.1f} |"
            )
        lines += ["", "## Key Actions by Milestone", ""]
        for m in roadmap.milestones:
            if m.key_actions:
                lines.append(f"### {m.year}")
                for action in m.key_actions:
                    lines.append(f"- {action}")
                lines.append("")
        return "\n".join(lines)
