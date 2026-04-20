"""
Geokodierung über Nominatim (OpenStreetMap).
Wandelt eine Adresse in Koordinaten um und ermittelt das Bundesland.
"""

import time
import logging
import requests
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Rate-Limiting: letzte Abfragezeit speichern
# TODO: threading.Lock erforderlich bei multi-worker Deployment
_last_request_time = 0.0

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "KatasterLookup/1.0 (Sachverstaendigenbuero Beier & Partner)"
}

# Bundesländer, die unser Service abdeckt
SUPPORTED_STATES = [
    "Niedersachsen",
    "Hamburg",
    "Bremen",
    "Schleswig-Holstein",
    "Mecklenburg-Vorpommern",
    "Nordrhein-Westfalen",
]


@dataclass
class GeocodingResult:
    """Ergebnis einer Geokodierung."""
    lat: float
    lon: float
    display_name: str
    bundesland: Optional[str]
    ort: Optional[str]
    plz: Optional[str]


def _rate_limit():
    """Nominatim erlaubt max. 1 Request/Sekunde."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    _last_request_time = time.time()


def geocode(adresse: str) -> Optional[GeocodingResult]:
    """
    Geokodiert eine Adresse über Nominatim.

    Args:
        adresse: Vollständige Adresse, z.B. "Musterstraße 1, 21680 Stade"

    Returns:
        GeocodingResult oder None, wenn die Adresse nicht gefunden wurde.
    """
    _rate_limit()

    params = {
        "q": adresse,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
        "countrycodes": "de",
    }

    try:
        response = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning("Nominatim-Abfrage fehlgeschlagen: %s", e)
        return None

    if not results:
        logger.info("Keine Ergebnisse fuer: %s", adresse)
        return None

    result = results[0]
    address_details = result.get("address", {})

    # Bundesland ermitteln
    # Für Stadtstaaten (Hamburg, Bremen) liefert Nominatim oft kein "state"-Feld,
    # weil die Stadt selbst das Bundesland ist.
    bundesland = address_details.get("state")

    if not bundesland:
        # Fallback für Stadtstaaten: prüfe ob city/town = Stadtstaat
        city = (address_details.get("city")
                or address_details.get("town")
                or address_details.get("village")
                or address_details.get("municipality")
                or "")
        city_lower = city.lower().strip()

        # Hamburg und Bremen sind Stadtstaaten
        if "hamburg" in city_lower:
            bundesland = "Hamburg"
        elif "bremen" in city_lower or "bremerhaven" in city_lower:
            bundesland = "Bremen"

    if not bundesland:
        # Letzter Fallback: display_name durchsuchen
        display = result.get("display_name", "")
        if "Hamburg" in display and "Niedersachsen" not in display:
            bundesland = "Hamburg"
        elif "Bremen" in display and "Niedersachsen" not in display:
            bundesland = "Bremen"

    logger.info("Bundesland ermittelt: %s", bundesland)

    return GeocodingResult(
        lat=float(result["lat"]),
        lon=float(result["lon"]),
        display_name=result.get("display_name", ""),
        bundesland=bundesland,
        ort=address_details.get("city")
           or address_details.get("town")
           or address_details.get("village")
           or address_details.get("municipality"),
        plz=address_details.get("postcode"),
    )


def is_supported(bundesland: Optional[str]) -> bool:
    """Prüft, ob das Bundesland von unserem Service abgedeckt wird."""
    if not bundesland:
        return False
    return bundesland in SUPPORTED_STATES
