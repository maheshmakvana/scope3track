"""Data models for scope3track — Carbon/Scope 3 emissions tracking."""
from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class EmissionScope(str, Enum):
    SCOPE1 = "scope1"   # Direct emissions
    SCOPE2 = "scope2"   # Purchased energy
    SCOPE3 = "scope3"   # Value chain (upstream + downstream)


class Scope3Category(str, Enum):
    PURCHASED_GOODS = "purchased_goods_services"
    CAPITAL_GOODS = "capital_goods"
    FUEL_ENERGY = "fuel_and_energy_activities"
    UPSTREAM_TRANSPORT = "upstream_transportation_distribution"
    WASTE = "waste_generated_in_operations"
    BUSINESS_TRAVEL = "business_travel"
    EMPLOYEE_COMMUTING = "employee_commuting"
    UPSTREAM_LEASED = "upstream_leased_assets"
    DOWNSTREAM_TRANSPORT = "downstream_transportation_distribution"
    PROCESSING = "processing_of_sold_products"
    USE_OF_PRODUCTS = "use_of_sold_products"
    END_OF_LIFE = "end_of_life_treatment"
    DOWNSTREAM_LEASED = "downstream_leased_assets"
    FRANCHISES = "franchises"
    INVESTMENTS = "investments"


class EmissionUnit(str, Enum):
    KG_CO2E = "kg_co2e"
    T_CO2E = "t_co2e"
    MT_CO2E = "mt_co2e"


class EmissionEntry(BaseModel):
    """A single emission data point."""

    entry_id: str
    scope: EmissionScope
    category: Optional[Scope3Category] = None
    source: str
    activity_amount: float = Field(ge=0)
    activity_unit: str
    emission_factor: float = Field(ge=0)
    emissions_kg_co2e: float = Field(ge=0)
    period_start: datetime
    period_end: datetime
    supplier_id: Optional[str] = None
    notes: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("entry_id", "source")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field must not be empty")
        return v.strip()


class SupplierEmissions(BaseModel):
    """Aggregated Scope 3 emissions for a supplier."""

    supplier_id: str
    supplier_name: str
    entries: List[EmissionEntry] = Field(default_factory=list)
    total_kg_co2e: float = 0.0
    reporting_year: int
    verified: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def total_by_category(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        for entry in self.entries:
            key = entry.category.value if entry.category else "uncategorized"
            result[key] = result.get(key, 0.0) + entry.emissions_kg_co2e
        return result


class EmissionReport(BaseModel):
    """Full emissions report across all scopes."""

    company_id: str
    reporting_year: int
    scope1_kg_co2e: float = 0.0
    scope2_kg_co2e: float = 0.0
    scope3_kg_co2e: float = 0.0
    scope3_by_category: Dict[str, float] = Field(default_factory=dict)
    entries: List[EmissionEntry] = Field(default_factory=list)
    suppliers: List[SupplierEmissions] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    framework: str = "GHG Protocol"

    @property
    def total_kg_co2e(self) -> float:
        return self.scope1_kg_co2e + self.scope2_kg_co2e + self.scope3_kg_co2e

    @property
    def total_t_co2e(self) -> float:
        return self.total_kg_co2e / 1000.0

    def summary(self) -> Dict[str, Any]:
        return {
            "company_id": self.company_id,
            "reporting_year": self.reporting_year,
            "scope1_t_co2e": round(self.scope1_kg_co2e / 1000, 3),
            "scope2_t_co2e": round(self.scope2_kg_co2e / 1000, 3),
            "scope3_t_co2e": round(self.scope3_kg_co2e / 1000, 3),
            "total_t_co2e": round(self.total_t_co2e, 3),
            "framework": self.framework,
        }
