"""scope3track — Carbon and Scope 3 emissions tracking for SMBs and enterprise supply chains."""
from scope3track.models import (
    EmissionEntry,
    EmissionReport,
    EmissionScope,
    EmissionUnit,
    Scope3Category,
    SupplierEmissions,
)
from scope3track.calculator import (
    EmissionCalculator,
    EmissionFactorRegistry,
    default_registry,
)
from scope3track.exceptions import (
    CalculationError,
    ReportError,
    Scope3TrackError,
    ValidationError,
)
from scope3track.advanced import (
    AuditLog,
    CancellationToken,
    EmissionCache,
    EmissionDiff,
    EmissionDriftDetector,
    EmissionPipeline,
    EmissionProfiler,
    EmissionReportExporter,
    EmissionRule,
    EmissionValidator,
    PIIScrubber,
    RateLimiter,
    abatch_calculate,
    batch_calculate,
    diff_entries,
    entries_to_ndjson,
    stream_entries,
)

__version__ = "1.0.0"
__all__ = [
    # Core
    "EmissionCalculator",
    "EmissionFactorRegistry",
    "default_registry",
    "EmissionEntry",
    "EmissionReport",
    "EmissionScope",
    "EmissionUnit",
    "Scope3Category",
    "SupplierEmissions",
    # Exceptions
    "Scope3TrackError",
    "CalculationError",
    "ValidationError",
    "ReportError",
    # Advanced
    "EmissionCache",
    "EmissionPipeline",
    "EmissionValidator",
    "EmissionRule",
    "RateLimiter",
    "CancellationToken",
    "batch_calculate",
    "abatch_calculate",
    "EmissionProfiler",
    "EmissionDriftDetector",
    "EmissionReportExporter",
    "stream_entries",
    "entries_to_ndjson",
    "EmissionDiff",
    "diff_entries",
    "AuditLog",
    "PIIScrubber",
]
