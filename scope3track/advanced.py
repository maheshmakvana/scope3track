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

from scope3track.models import EmissionEntry, EmissionReport

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
                import pickle as pk
                pk.dump(dict(self._store), f)

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
