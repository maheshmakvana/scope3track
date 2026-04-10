"""Exceptions for scope3track."""


class Scope3TrackError(Exception):
    """Base exception for scope3track."""


class CalculationError(Scope3TrackError):
    """Raised when an emission calculation fails."""


class ValidationError(Scope3TrackError):
    """Raised on invalid emission data."""


class ReportError(Scope3TrackError):
    """Raised on report generation failure."""
