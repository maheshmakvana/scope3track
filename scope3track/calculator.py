"""Emission calculation engine for scope3track."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

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
    "kwh_electricity_us": 0.386,
    "kwh_electricity_eu": 0.233,
    "kwh_natural_gas": 0.202,
    # Transport
    "km_road_freight_hgv": 0.162,
    "km_air_freight_kg": 0.00602,
    "km_sea_freight_teu": 0.0113,
    "km_passenger_car": 0.171,
    "km_business_flight_economy": 0.255,
    # Materials
    "kg_steel": 2.09,
    "kg_aluminium": 8.24,
    "kg_plastic": 3.14,
    "kg_paper": 0.92,
    "kg_concrete": 0.159,
    # Waste
    "kg_landfill": 0.579,
    "kg_recycled": 0.021,
    "kg_composted": 0.010,
}


class EmissionFactorRegistry:
    """Registry for emission factors with override support."""

    def __init__(self) -> None:
        self._factors: Dict[str, float] = dict(DEFAULT_EMISSION_FACTORS)

    def get(self, key: str) -> float:
        if key not in self._factors:
            raise CalculationError(f"Unknown emission factor key: '{key}'. Register it with .register().")
        return self._factors[key]

    def register(self, key: str, kg_co2e_per_unit: float) -> None:
        if kg_co2e_per_unit < 0:
            raise CalculationError("Emission factor must be >= 0")
        self._factors[key] = kg_co2e_per_unit
        logger.debug("Registered emission factor %s = %s kg CO2e/unit", key, kg_co2e_per_unit)

    def list_keys(self) -> List[str]:
        return sorted(self._factors.keys())


default_registry = EmissionFactorRegistry()


class EmissionCalculator:
    """Calculate scope 1/2/3 emissions from activity data."""

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
        **kwargs: object,
    ) -> EmissionEntry:
        """Compute emissions for a single activity."""
        if custom_factor is not None:
            factor = custom_factor
        elif factor_key is not None:
            factor = self._registry.get(factor_key)
        else:
            raise CalculationError("Provide either factor_key or custom_factor")

        emissions_kg = activity_amount * factor

        from datetime import datetime
        entry = EmissionEntry(
            entry_id=entry_id,
            scope=scope,
            category=category,
            source=source,
            activity_amount=activity_amount,
            activity_unit=activity_unit,
            emission_factor=factor,
            emissions_kg_co2e=emissions_kg,
            period_start=kwargs.get("period_start", datetime.utcnow()),  # type: ignore[arg-type]
            period_end=kwargs.get("period_end", datetime.utcnow()),      # type: ignore[arg-type]
            supplier_id=kwargs.get("supplier_id"),                       # type: ignore[arg-type]
            notes=kwargs.get("notes", ""),                               # type: ignore[arg-type]
        )
        logger.debug("Calculated %s: %.4f kg CO2e (factor=%.4f, amount=%.4f)", source, emissions_kg, factor, activity_amount)
        return entry

    def build_report(
        self,
        company_id: str,
        reporting_year: int,
        entries: List[EmissionEntry],
        suppliers: Optional[List[SupplierEmissions]] = None,
    ) -> EmissionReport:
        """Aggregate entries into a full EmissionReport."""
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
        )
        logger.info(
            "Report built for %s (%d): total=%.2f t CO2e",
            company_id, reporting_year, report.total_t_co2e
        )
        return report
