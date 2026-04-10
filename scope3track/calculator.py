"""Emission calculation engine for scope3track."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from scope3track.exceptions import CalculationError
from scope3track.models import (
    EmissionEntry,
    EmissionReport,
    EmissionScope,
    Scope3Category,
    SupplierEmissions,
)

logger = logging.getLogger(__name__)

# Built-in emission factors (kg CO2e per unit) — GHG Protocol defaults
DEFAULT_EMISSION_FACTORS: Dict[str, float] = {
    # Energy
    "kwh_electricity_us":        0.386,
    "kwh_electricity_eu":        0.233,
    "kwh_electricity_global":    0.475,
    "kwh_natural_gas":           0.202,
    "kwh_renewable":             0.000,
    # Transport
    "km_road_freight_hgv":       0.162,
    "km_road_freight_van":       0.280,
    "km_air_freight_kg":         0.00602,
    "km_sea_freight_teu":        0.0113,
    "km_rail_freight_tonne":     0.028,
    "km_passenger_car":          0.171,
    "km_business_flight_economy": 0.255,
    "km_business_flight_business": 0.765,
    # Materials
    "kg_steel":                  2.09,
    "kg_aluminium":              8.24,
    "kg_plastic":                3.14,
    "kg_paper":                  0.92,
    "kg_concrete":               0.159,
    "kg_glass":                  0.85,
    "kg_cotton":                 5.50,
    # Waste
    "kg_landfill":               0.579,
    "kg_recycled":               0.021,
    "kg_composted":              0.010,
    "kg_incinerated":            0.210,
    # Spend-based (kg CO2e per USD spend) — EEIO approximations
    "usd_manufacturing":         0.350,
    "usd_services":              0.120,
    "usd_agriculture":           0.800,
    "usd_it_services":           0.180,
    "usd_logistics":             0.290,
    "usd_construction":          0.420,
}

# Uncertainty factors by data quality tier (% of emissions as standard deviation)
_DATA_QUALITY_UNCERTAINTY: Dict[str, float] = {
    "primary":   0.05,   # verified primary data
    "secondary": 0.15,   # industry-average factors
    "estimated": 0.30,   # spend-based or modelled
    "default":   0.20,
}


class EmissionFactorRegistry:
    """Registry for emission factors with override and versioning support."""

    def __init__(self) -> None:
        self._factors: Dict[str, float] = dict(DEFAULT_EMISSION_FACTORS)
        self._custom_keys: set = set()

    def get(self, key: str) -> float:
        """Return the emission factor for a key; raises CalculationError if not found."""
        if key not in self._factors:
            raise CalculationError(
                f"Unknown emission factor key: '{key}'. "
                f"Available keys: {', '.join(sorted(self._factors)[:10])}… "
                "Register custom factors with .register()."
            )
        return self._factors[key]

    def register(self, key: str, kg_co2e_per_unit: float) -> None:
        """Register or override an emission factor."""
        if kg_co2e_per_unit < 0:
            raise CalculationError("Emission factor must be >= 0")
        self._factors[key] = kg_co2e_per_unit
        self._custom_keys.add(key)
        logger.debug("Registered emission factor %s = %.6f kg CO2e/unit", key, kg_co2e_per_unit)

    def bulk_register(self, factors: Dict[str, float]) -> int:
        """Register multiple factors at once; returns count added."""
        for key, value in factors.items():
            self.register(key, value)
        return len(factors)

    def list_keys(self) -> List[str]:
        """Return all registered factor keys."""
        return sorted(self._factors.keys())

    def custom_keys(self) -> List[str]:
        """Return only user-registered (non-default) keys."""
        return sorted(self._custom_keys)

    def search(self, query: str) -> Dict[str, float]:
        """Return factors whose keys contain the query string."""
        q = query.lower()
        return {k: v for k, v in self._factors.items() if q in k}

    def export(self) -> Dict[str, float]:
        """Return a snapshot of all registered factors."""
        return dict(self._factors)


default_registry = EmissionFactorRegistry()


class EmissionCalculator:
    """
    Calculate Scope 1/2/3 emissions from activity data.

    Supports both factor-key based calculation and direct custom-factor input.
    Tracks data quality tiers and propagates uncertainty estimates through the
    report to support GHG Protocol data quality scoring.
    """

    def __init__(self, registry: Optional[EmissionFactorRegistry] = None) -> None:
        self._registry = registry or default_registry

    def calculate(
        self,
        entry_id: str,
        scope: EmissionScope,
        source: str,
        activity_amount: float,
        activity_unit: str,
        factor_key: Optional[str] = None,
        custom_factor: Optional[float] = None,
        category: Optional[Scope3Category] = None,
        data_quality: str = "default",
        **kwargs: object,
    ) -> EmissionEntry:
        """
        Compute emissions for a single activity.

        Args:
            entry_id: Unique identifier for this emission record.
            scope: EmissionScope (SCOPE1, SCOPE2, SCOPE3).
            source: Description of the emission source.
            activity_amount: Quantity of the activity (e.g., kWh, km, kg).
            activity_unit: Unit of the activity amount.
            factor_key: Key in the EmissionFactorRegistry.
            custom_factor: Direct emission factor override (kg CO2e / unit).
            category: Scope3Category for Scope 3 entries.
            data_quality: "primary", "secondary", "estimated", or "default".
            **kwargs: period_start, period_end, supplier_id, notes, metadata.
        """
        if activity_amount < 0:
            raise CalculationError(f"activity_amount must be >= 0; got {activity_amount}")

        if custom_factor is not None:
            if custom_factor < 0:
                raise CalculationError("custom_factor must be >= 0")
            factor = custom_factor
        elif factor_key is not None:
            factor = self._registry.get(factor_key)
        else:
            raise CalculationError(
                "Provide either factor_key (from EmissionFactorRegistry) or custom_factor."
            )

        emissions_kg = activity_amount * factor

        # Uncertainty estimate for data quality reporting
        uncertainty_pct = _DATA_QUALITY_UNCERTAINTY.get(data_quality, _DATA_QUALITY_UNCERTAINTY["default"])
        uncertainty_kg = emissions_kg * uncertainty_pct

        period_start = kwargs.get("period_start", datetime.utcnow())
        period_end = kwargs.get("period_end", datetime.utcnow())
        supplier_id = kwargs.get("supplier_id")
        notes = kwargs.get("notes", "")
        metadata = dict(kwargs.get("metadata", {}))  # type: ignore[arg-type]
        metadata["data_quality"] = data_quality
        metadata["uncertainty_kg_co2e"] = round(uncertainty_kg, 6)

        entry = EmissionEntry(
            entry_id=entry_id,
            scope=scope,
            category=category,
            source=source,
            activity_amount=activity_amount,
            activity_unit=activity_unit,
            emission_factor=factor,
            emissions_kg_co2e=emissions_kg,
            period_start=period_start,  # type: ignore[arg-type]
            period_end=period_end,      # type: ignore[arg-type]
            supplier_id=supplier_id,    # type: ignore[arg-type]
            notes=str(notes),
            metadata=metadata,
        )
        logger.debug(
            "Calculated %s [%s]: %.4f kg CO2e (factor=%.6f × amount=%.4f, quality=%s)",
            source, scope.value, emissions_kg, factor, activity_amount, data_quality,
        )
        return entry

    def calculate_spend_based(
        self,
        entry_id: str,
        spend_usd: float,
        spend_category: str,
        scope: EmissionScope = EmissionScope.SCOPE3,
        category: Optional[Scope3Category] = Scope3Category.PURCHASED_GOODS,
        source: str = "spend-based estimation",
        **kwargs: object,
    ) -> EmissionEntry:
        """
        Compute Scope 3 emissions using spend-based EEIO factors.

        Use when primary or secondary activity data is unavailable. Looks up
        a 'usd_<spend_category>' key in the registry.

        Args:
            entry_id: Unique entry identifier.
            spend_usd: USD spend amount.
            spend_category: Category key suffix (e.g. "manufacturing", "services").
            scope: Scope of the emission (default SCOPE3).
            category: Scope3Category.
            source: Description of the spend source.
        """
        factor_key = f"usd_{spend_category}"
        return self.calculate(
            entry_id=entry_id,
            scope=scope,
            source=source,
            activity_amount=spend_usd,
            activity_unit="USD",
            factor_key=factor_key,
            category=category,
            data_quality="estimated",
            **kwargs,
        )

    def bulk_calculate(
        self,
        activities: List[Dict],
        max_workers: int = 4,
    ) -> Tuple[List[EmissionEntry], List[Dict]]:
        """
        Calculate emissions for a batch of activity dicts concurrently.

        Each dict in `activities` must contain the same kwargs as `calculate()`.
        Returns (successful_entries, failed_records) where failed_records include
        the original dict and the error message.
        """
        successful: List[EmissionEntry] = []
        failed: List[Dict] = []

        def _calc_one(activity: Dict) -> EmissionEntry:
            return self.calculate(**activity)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_calc_one, a): a for a in activities}
            for future in as_completed(futures):
                activity = futures[future]
                try:
                    successful.append(future.result())
                except Exception as exc:
                    failed.append({"activity": activity, "error": str(exc)})
                    logger.error("bulk_calculate: failed entry_id=%s: %s", activity.get("entry_id"), exc)

        logger.info("bulk_calculate: %d succeeded, %d failed", len(successful), len(failed))
        return successful, failed

    def build_report(
        self,
        company_id: str,
        reporting_year: int,
        entries: List[EmissionEntry],
        suppliers: Optional[List[SupplierEmissions]] = None,
        framework: str = "GHG Protocol",
    ) -> EmissionReport:
        """
        Aggregate emission entries into a complete EmissionReport.

        Automatically computes Scope 1/2/3 totals, category breakdowns,
        and cross-references supplier data if provided.
        """
        scope1 = sum(e.emissions_kg_co2e for e in entries if e.scope == EmissionScope.SCOPE1)
        scope2 = sum(e.emissions_kg_co2e for e in entries if e.scope == EmissionScope.SCOPE2)
        scope3_entries = [e for e in entries if e.scope == EmissionScope.SCOPE3]
        scope3 = sum(e.emissions_kg_co2e for e in scope3_entries)

        scope3_by_cat: Dict[str, float] = {}
        for e in scope3_entries:
            key = e.category.value if e.category else "uncategorized"
            scope3_by_cat[key] = scope3_by_cat.get(key, 0.0) + e.emissions_kg_co2e

        report = EmissionReport(
            company_id=company_id,
            reporting_year=reporting_year,
            scope1_kg_co2e=scope1,
            scope2_kg_co2e=scope2,
            scope3_kg_co2e=scope3,
            scope3_by_category=scope3_by_cat,
            entries=entries,
            suppliers=suppliers or [],
            framework=framework,
        )
        logger.info(
            "Report built for %s (%d): scope1=%.2f t, scope2=%.2f t, scope3=%.2f t, total=%.2f t CO2e",
            company_id, reporting_year,
            scope1 / 1000, scope2 / 1000, scope3 / 1000, report.total_t_co2e,
        )
        return report

    def intensity_metrics(
        self,
        report: EmissionReport,
        revenue_usd: Optional[float] = None,
        employees: Optional[int] = None,
        production_units: Optional[float] = None,
    ) -> Dict[str, Optional[float]]:
        """
        Compute emissions intensity metrics for normalised reporting.

        Returns t CO2e per $ revenue, per employee, and per production unit
        (any that have the denominator provided). Common in GRI, CDP, and TCFD
        reporting frameworks.
        """
        total_t = report.total_t_co2e
        return {
            "total_t_co2e": round(total_t, 3),
            "t_co2e_per_million_usd_revenue": round(total_t / (revenue_usd / 1_000_000), 3) if revenue_usd else None,
            "t_co2e_per_employee": round(total_t / employees, 3) if employees else None,
            "t_co2e_per_unit_produced": round(total_t / production_units, 6) if production_units else None,
        }

    def data_quality_summary(self, entries: List[EmissionEntry]) -> Dict[str, Any]:
        """
        Summarise data quality distribution and total uncertainty across entries.

        Returns counts by quality tier and the aggregate uncertainty range
        to include in GHG Protocol assurance disclosures.
        """
        from typing import Any
        quality_counts: Dict[str, int] = {}
        total_uncertainty = 0.0
        total_kg = 0.0

        for e in entries:
            dq = e.metadata.get("data_quality", "default") if e.metadata else "default"
            quality_counts[dq] = quality_counts.get(dq, 0) + 1
            uncertainty_kg = e.metadata.get("uncertainty_kg_co2e", 0.0) if e.metadata else 0.0
            total_uncertainty += float(uncertainty_kg)
            total_kg += e.emissions_kg_co2e

        return {
            "total_entries": len(entries),
            "quality_distribution": quality_counts,
            "total_kg_co2e": round(total_kg, 3),
            "total_uncertainty_kg_co2e": round(total_uncertainty, 3),
            "uncertainty_pct": round(total_uncertainty / total_kg * 100, 2) if total_kg > 0 else 0.0,
        }
