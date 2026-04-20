"""
OGC API Features Client für Nordrhein-Westfalen (Geobasis NRW / IT.NRW).

Endpunkt: https://ogc-api.nrw.de/lika/v1
Collections: flurstueck, gebaeude_bauwerk, katasterbezirk, nutzung, verwaltungseinheit

Attributnamen sind identisch zum LGLN-Schema (Kurznamen):
  flstkennz(?), gemarkung, gemaschl, flur, flstnrzae, flstnrnen,
  flaeche, lagebeztxt, tntxt, kreis, gemeinde, aktualit

Lizenz: Datenlizenz Deutschland – Zero – Version 2.0 (komplett frei!)
"""

import re
import requests
from typing import Optional, List
from wfs_clients import WFSClient, FlurstueckInfo

OAF_BASE_URL = "https://ogc-api.nrw.de/lika/v1"
OAF_FLURSTUECK_URL = f"{OAF_BASE_URL}/collections/flurstueck/items"
OAF_GEBAEUDE_URL = f"{OAF_BASE_URL}/collections/gebaeude_bauwerk/items"

HEADERS = {
    "User-Agent": "KatasterLookup/1.0",
    "Accept": "application/geo+json",
}


class NordrheinWestfalenClient(WFSClient):
    """OGC API Features Client für Nordrhein-Westfalen."""

    @property
    def bundesland_name(self) -> str:
        return "Nordrhein-Westfalen"

    def query_flurstueck(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """Sucht das beste einzelne Flurstück."""
        results = self.query_flurstuecke(lat, lon, adresse)
        return results[0] if results else None

    def query_flurstuecke(self, lat: float, lon: float, adresse: str = "") -> List[FlurstueckInfo]:
        """Sucht ALLE passenden Flurstücke über die OGC API Features."""
        buffer_deg = 0.0003
        bbox = f"{lon - buffer_deg},{lat - buffer_deg},{lon + buffer_deg},{lat + buffer_deg}"

        params = {
            "bbox": bbox,
            "limit": 10,
            "f": "json",
        }

        try:
            response = requests.get(
                OAF_FLURSTUECK_URL,
                params=params,
                headers=HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"[NW] OAF-Fehler: {e}")
            return []
        except ValueError as e:
            print(f"[NW] JSON-Parse-Fehler: {e}")
            return []

        features = data.get("features", [])
        if not features:
            print(f"[NW] Keine Flurstücke gefunden bei {lat}, {lon}")
            return []

        print(f"[NW] {len(features)} Flurstück(e) in der Antwort")

        results = []
        for feat in features:
            info = self._parse_feature(feat)
            if info:
                results.append(info)

        if not results:
            return []

        # Nach Lagebezeichnung filtern
        if adresse:
            return self.filter_by_address(results, adresse)

        return [results[0]]

    def _parse_feature(self, feature: dict) -> Optional[FlurstueckInfo]:
        """Parst ein GeoJSON-Feature in FlurstueckInfo."""
        props = feature.get("properties", {})

        info = FlurstueckInfo()
        info.bundesland = "Nordrhein-Westfalen"
        info.quelle = "Geobasis NRW, Open Data (Datenlizenz DE-Zero 2.0)"

        # Attribute — LGLN-Kurznamen (identisch zu NI/HH)
        info.gemarkung = props.get("gemarkung")
        info.gemarkungsnummer = props.get("gemaschl")

        # Flurnummer
        flur = props.get("flur")
        if flur:
            info.flur = str(flur).lstrip("0") or "0"

        # Flurstücksnummer
        zae = props.get("flstnrzae")
        info.flurstueck_zaehler = str(zae) if zae else None
        nen = props.get("flstnrnen")
        info.flurstueck_nenner = str(nen) if nen else None

        info.flurstueckskennzeichen = props.get("flstkennz")
        # Fallback: Kennzeichen aus Schlüsseln zusammenbauen
        if not info.flurstueckskennzeichen and info.gemarkungsnummer:
            flurschl = props.get("flurschl", "")
            if flurschl:
                info.flurstueckskennzeichen = flurschl

        # Fläche
        flaeche = props.get("flaeche")
        if flaeche is not None:
            try:
                info.amtliche_flaeche = float(flaeche)
            except (ValueError, TypeError):
                pass

        # Zusatzinfos
        info.lagebezeichnung = props.get("lagebeztxt")
        info.gemeinde = props.get("gemeinde")
        info.kreis = props.get("kreis")
        info.nutzungsart = props.get("tntxt")
        info.aktualitaet = props.get("aktualit")

        print(f"[NW]   -> {info.gemarkung} Flur {info.flur} "
              f"Flurstück {info.flurstueck_display} "
              f"({info.flaeche_display}) Lage: {info.lagebezeichnung}")
        return info

    def query_gebaeude(self, lat: float, lon: float) -> Optional[float]:
        """Ermittelt die Gebäudegrundfläche über die OGC API."""
        buffer_deg = 0.0001
        bbox = f"{lon - buffer_deg},{lat - buffer_deg},{lon + buffer_deg},{lat + buffer_deg}"

        params = {
            "bbox": bbox,
            "limit": 20,
            "f": "json",
        }

        try:
            response = requests.get(
                OAF_GEBAEUDE_URL,
                params=params,
                headers=HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            return None

        features = data.get("features", [])
        if not features:
            return None

        total_area = 0.0
        for feat in features:
            props = feat.get("properties", {})
            gf = props.get("grundflaeche") or props.get("flaeche")
            if gf:
                try:
                    total_area += float(gf)
                except (ValueError, TypeError):
                    pass

        return total_area if total_area > 0 else None

    @staticmethod
    def _match_by_address(results: List[FlurstueckInfo], adresse: str) -> Optional[FlurstueckInfo]:
        """Findet das beste Match über die Lagebezeichnung."""
        match = re.search(r'(\d+)\s*([a-zA-Z])?', adresse)
        if not match:
            return None

        input_number = match.group(1)
        input_suffix = (match.group(2) or "").upper()
        input_full = f"{input_number}{input_suffix}".strip()

        print(f"[NW] Adress-Matching: suche '{input_full}' in {len(results)} Flurstücken")

        best_match = None

        for info in results:
            if not info.lagebezeichnung:
                continue
            # NRW lagebeztxt kann mehrere Adressen enthalten, getrennt durch ";"
            lage_parts = info.lagebezeichnung.replace(";", " ")
            lage_numbers = re.findall(r'(\d+)\s*([a-zA-Z])?(?:\s|$|,|;)', lage_parts)
            for num, suffix in lage_numbers:
                lage_full = f"{num}{suffix.upper()}".strip()
                if lage_full == input_full:
                    print(f"[NW]   Exakter Treffer: '{info.lagebezeichnung}' -> {info.flurstueck_display}")
                    return info
                elif num == input_number and not best_match:
                    best_match = info

        return best_match
