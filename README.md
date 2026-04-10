# scope3track

**Carbon and Scope 3 emissions tracking for SMBs and enterprise supply chains** — GHG Protocol-compliant calculations, supplier-level tracking, CSRD-ready reporting.

Enterprise carbon tools cost $30K–$150K/year. `scope3track` brings the same capability to Python developers for free.

[![PyPI version](https://badge.fury.io/py/scope3track.svg)](https://pypi.org/project/scope3track/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

## The Problem

EU CSRD enforcement + enterprise Scope 3 requirements are now cascading to SMB suppliers. Companies are required to report their full value-chain emissions. Enterprise tools cost $30K–$150K/year — nothing exists at the $99–$499/mo SMB price point.

## Installation

```bash
pip install scope3track
```

## Quick Start

```python
from scope3track import EmissionCalculator, EmissionScope, Scope3Category
from datetime import datetime

calc = EmissionCalculator()

# Calculate Scope 3 Category 4: upstream transport
transport_entry = calc.calculate(
    entry_id="ENT-001",
    scope=EmissionScope.SCOPE3,
    category=Scope3Category.UPSTREAM_TRANSPORT,
    source="Freight — Shanghai to LA",
    activity_amount=5000,    # kg of freight
    activity_unit="kg",
    factor_key="km_road_freight_hgv",
    period_start=datetime(2025, 1, 1),
    period_end=datetime(2025, 3, 31),
    supplier_id="SUPPLIER-CN-01",
)

print(f"{transport_entry.emissions_kg_co2e:.2f} kg CO2e")
```

## Build a Full Report

```python
entries = [
    calc.calculate("e1", EmissionScope.SCOPE1, "Natural gas heating", 10000, "kwh", "kwh_natural_gas", period_start=..., period_end=...),
    calc.calculate("e2", EmissionScope.SCOPE2, "Office electricity", 50000, "kwh", "kwh_electricity_us", period_start=..., period_end=...),
    calc.calculate("e3", EmissionScope.SCOPE3, "Employee commuting", 200000, "km", "km_passenger_car", category=Scope3Category.EMPLOYEE_COMMUTING, period_start=..., period_end=...),
]

report = calc.build_report(
    company_id="ACME-INC",
    reporting_year=2025,
    entries=entries,
)

print(report.summary())
# {'company_id': 'ACME-INC', 'scope1_t_co2e': 2.02, 'scope2_t_co2e': 19.3, 'scope3_t_co2e': 34.2, 'total_t_co2e': 55.52, ...}
```

## Built-in Emission Factors

`scope3track` ships with GHG Protocol default factors for:

- **Energy**: electricity (US/EU grid), natural gas
- **Transport**: road freight, air freight, sea freight, passenger car, business flights
- **Materials**: steel, aluminium, plastic, paper, concrete
- **Waste**: landfill, recycled, composted

Register custom factors:

```python
from scope3track import default_registry

default_registry.register("kwh_solar_ppa", 0.012)
```

## Scope 3 Categories Supported

All 15 GHG Protocol Scope 3 categories (upstream + downstream):

`purchased_goods_services`, `capital_goods`, `fuel_and_energy_activities`, `upstream_transportation_distribution`, `waste_generated_in_operations`, `business_travel`, `employee_commuting`, `upstream_leased_assets`, `downstream_transportation_distribution`, `processing_of_sold_products`, `use_of_sold_products`, `end_of_life_treatment`, `downstream_leased_assets`, `franchises`, `investments`

## Advanced Features

### Pipeline

```python
from scope3track import EmissionPipeline

pipeline = (
    EmissionPipeline()
    .filter(lambda e: e.scope == EmissionScope.SCOPE3, name="scope3_only")
    .map(lambda entries: sorted(entries, key=lambda e: -e.emissions_kg_co2e), name="sort_by_impact")
    .with_retry(count=2)
)

scope3_sorted = pipeline.run(entries)
```

### Caching

```python
from scope3track import EmissionCache

cache = EmissionCache(max_size=1000, ttl_seconds=3600)

@cache.memoize
def get_supplier_emissions(supplier_id):
    ...

cache.save("emissions_cache.pkl")
cache.load("emissions_cache.pkl")
print(cache.stats())
```

### Export Reports

```python
from scope3track import EmissionReportExporter

print(EmissionReportExporter.to_json(report))
print(EmissionReportExporter.to_csv(report))
print(EmissionReportExporter.to_markdown(report))
```

### Drift Detection

```python
from scope3track import EmissionDriftDetector

detector = EmissionDriftDetector(threshold=0.15)
for period_total in quarterly_totals:
    detector.record(period_total)

if detector.is_drifted():
    print(f"Emission drift detected: {detector.drift_ratio():.1%}")
```

### Diff Between Periods

```python
from scope3track import diff_entries

diff = diff_entries(q1_entries, q2_entries)
print(diff.summary())  # {'added': 2, 'removed': 0, 'modified': 3}
print(diff.to_json())
```

### Async Batch

```python
from scope3track import abatch_calculate, CancellationToken

token = CancellationToken()
entries = await abatch_calculate(raw_data, calc_fn, max_concurrency=8, token=token)
```

### Streaming NDJSON

```python
from scope3track import entries_to_ndjson

for line in entries_to_ndjson(entries):
    output_stream.write(line)
```

## Frameworks Supported

- GHG Protocol Corporate Standard
- EU CSRD / ESRS E1
- ISO 14064-1

## Changelog

### v1.2.2 (2026-04-10)
- Added Contributing and Author sections to README

### v1.2.1 (2026-04-10)
- Added Changelog section to README for release traceability

### v1.2.0
- Added `EmissionHotspotAnalyzer` — identify top emission contributors across supply chain tiers
- Added `NetZeroRoadmapGenerator` — generate phased net-zero reduction roadmaps with milestones
- Expanded SEO keywords for PyPI discoverability

### v1.0.1
- Advanced features: pipeline, caching, validation, diff/trend, streaming, audit log

### v1.0.0
- Initial release: Scope 3 emissions tracking, GHG Protocol calculations, supplier-level reporting

## License

MIT

## Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository on [GitHub](https://github.com/maheshmakvana/scope3track)
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes and add tests
4. Run the test suite: `pytest tests/ -v`
5. Submit a pull request

Please open an issue first for major changes to discuss the approach.

## Author

**Mahesh Makvana** — [GitHub](https://github.com/maheshmakvana) · [PyPI](https://pypi.org/user/maheshmakvana/)

MIT License
