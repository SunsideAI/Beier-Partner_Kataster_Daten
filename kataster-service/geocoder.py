"""
Geokodierung über Nominatim (OpenStreetMap).
Wandelt eine Adresse in Koordinaten um und ermittelt das Bundesland.
"""

import re
import time
import logging
import requests
from dataclasses import dataclass, field
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

# Adresse in PLZ-getrennte Komponenten zerlegen (für structured Nominatim query)
_STRUCT_RE = re.compile(r'^(.+?),\s*(\d{5})\s+([^,]+)(?:,.*)?$')

# Hausnummer + optionaler Buchstaben-Suffix aus Adresse extrahieren
_HN_RE = re.compile(r'\b(\d+)\s*([a-zA-Z])?\b')

# Nominatim addresstype-Werte, die eindeutig kein Haus-Match sind
_ROAD_TYPES = {"road", "street", "path", "cycleway", "footway", "pedestrian"}


@dataclass
class GeocodingResult:
    """Ergebnis einer Geokodierung."""
    lat: float
    lon: float
    display_name: str
    bundesland: Optional[str]
    ort: Optional[str]
    plz: Optional[str]
    house_number_matched: bool = True
    # Debug-Felder (für /test-adresse Endpoint)
    addresstype: str = ""
    nominatim_house_number: Optional[str] = None
    query_mode: str = "freeform"
    expected_house_number: Optional[str] = None


def _rate_limit():
    """Nominatim erlaubt max. 1 Request/Sekunde."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    _last_request_time = time.time()


def _extract_expected_house_number(adresse: str) -> Optional[str]:
    """Extrahiert die erwartete Hausnummer + Suffix aus der Input-Adresse.
    Gibt None zurück wenn keine Hausnummer vorhanden.
    """
    match = _HN_RE.search(adresse)
    if not match:
        return None
    number = match.group(1)
    suffix = (match.group(2) or "").upper()
    return f"{number}{suffix}"


def _check_house_number_matched(
    addresstype: str,
    nominatim_house_number: Optional[str],
    expected_full: Optional[str],
) -> bool:
    """Entscheidet ob die Hausnummer sicher aufgelöst wurde."""
    # Starkes Negativ-Signal: Nominatim hat explizit nur eine Straße gefunden
    if addresstype in _ROAD_TYPES:
        return False

    # Kein Hausnummer im Input → kein Vergleich möglich, OK
    if not expected_full:
        return True

    # Nominatim hat Hausnummer zurückgegeben → direkt vergleichen
    if nominatim_house_number:
        nom = nominatim_house_number.replace(" ", "").upper()
        return nom == expected_full.upper()

    # Hausnummer erwartet, aber Nominatim hat keine zurückgegeben
    return False


def geocode(adresse: str) -> Optional[GeocodingResult]:
    """
    Geokodiert eine Adresse über Nominatim.

    Versucht zuerst den strukturierten Query-Modus (präziser bei Hausnummern),
    fällt auf freeform zurück wenn die Adresse nicht geparst werden kann.

    Args:
        adresse: Vollständige Adresse, z.B. "Musterstraße 1, 21680 Stade"

    Returns:
        GeocodingResult oder None, wenn die Adresse nicht gefunden wurde.
        GeocodingResult.house_number_matched == False wenn Hausnummer nicht
        eindeutig aufgelöst werden konnte.
    """
    _rate_limit()

    # Versuche strukturierten Modus (PLZ als Ankerpunkt)
    struct_match = _STRUCT_RE.match(adresse.strip())
    if struct_match:
        params = {
            "street": struct_match.group(1),
            "postalcode": struct_match.group(2),
            "city": struct_match.group(3),
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
            "countrycodes": "de",
        }
        query_mode = "structured"
    else:
        params = {
            "q": adresse,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
            "countrycodes": "de",
        }
        query_mode = "freeform"

    logger.info("Geocoder query_mode=%s: %s", query_mode, adresse)

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

    # Wenn strukturierter Modus nichts liefert → freeform Fallback
    if not results and query_mode == "structured":
        logger.info("Geocoder: structured liefert kein Ergebnis, freeform Fallback")
        try:
            fallback_params = {
                "q": adresse,
                "format": "jsonv2",
                "addressdetails": 1,
                "limit": 1,
                "countrycodes": "de",
            }
            response = requests.get(
                NOMINATIM_URL,
                params=fallback_params,
                headers=NOMINATIM_HEADERS,
                timeout=10,
            )
            response.raise_for_status()
            results = response.json()
            query_mode = "freeform"
        except (requests.RequestException, ValueError) as e:
            logger.warning("Nominatim-Fallback fehlgeschlagen: %s", e)
            return None

    if not results:
        logger.info("Keine Ergebnisse fuer: %s", adresse)
        return None

    result = results[0]
    address_details = result.get("address", {})
    addresstype = result.get("addresstype", "")
    nominatim_house_number = address_details.get("house_number") or None

    # Erwartete Hausnummer aus Input ableiten
    expected_full = _extract_expected_house_number(adresse)

    # Prüfen ob Hausnummer sicher aufgelöst wurde
    house_number_matched = _check_house_number_matched(
        addresstype, nominatim_house_number, expected_full
    )

    if not house_number_matched:
        logger.warning(
            "Hausnummer nicht aufgeloest: erwartet='%s' nominatim='%s' addresstype='%s'",
            expected_full, nominatim_house_number, addresstype,
        )

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

    logger.info(
        "Geocoder: %s | bundesland=%s | hn_matched=%s | addresstype=%s",
        adresse, bundesland, house_number_matched, addresstype,
    )

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
        house_number_matched=house_number_matched,
        addresstype=addresstype,
        nominatim_house_number=nominatim_house_number,
        query_mode=query_mode,
        expected_house_number=expected_full,
    )


def is_supported(bundesland: Optional[str]) -> bool:
    """Prüft, ob das Bundesland von unserem Service abgedeckt wird."""
    if not bundesland:
        return False
    return bundesland in SUPPORTED_STATES
