"""Reservoir configuration registry.

Add a new entry to RESERVOIRS to support any additional water body.
The pipeline reads CRS, bounding box, and known bloom periods from here.
No code changes needed for new reservoirs — only this file.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RESERVOIRS: dict[str, dict] = {
    "serre_poncon": {
        "name":        "Lac de Serre-Ponçon",
        "country":     "France",
        "geojson":     PROJECT_ROOT / "data" / "reservoir" / "serre_poncon.gpkg",
        "epsg":        "EPSG:32631",
        "bbox":        [6.27, 44.46, 6.43, 44.54],
        "area_km2":    28,
        "known_blooms": [
            {"start": "2023-07-01", "end": "2023-08-31", "label": "Jul-Aug 2023",
             "source": "NAIADES / field reports"},
            {"start": "2024-06-01", "end": "2024-08-31", "label": "Jun-Aug 2024",
             "source": "field reports"},
        ],
    },
    "entrepenhas": {
        "name":        "Embalse de Entrepeñas",
        "country":     "Spain",
        "geojson":     PROJECT_ROOT / "data" / "reservoir" / "entrepenhas.gpkg",
        "epsg":        "EPSG:32630",
        "bbox":        [-2.75, 40.49, -2.64, 40.66],
        "area_km2":    80,
        "known_blooms": [
            {"start": "2022-07-01", "end": "2022-09-30", "label": "Jul-Sep 2022",
             "source": "CHT reports (Confederación Hidrográfica del Tajo)"},
            {"start": "2023-07-01", "end": "2023-09-30", "label": "Jul-Sep 2023",
             "source": "CHT reports"},
        ],
    },
}


def get_reservoir(name: str) -> dict:
    """Return reservoir config dict; raise ValueError for unknown names."""
    if name not in RESERVOIRS:
        raise ValueError(
            f"Unknown reservoir '{name}'. Known: {', '.join(RESERVOIRS)}"
        )
    return RESERVOIRS[name]
