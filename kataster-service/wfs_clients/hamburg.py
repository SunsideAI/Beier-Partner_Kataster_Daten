"""
OGC API Features Client für Hamburg (LGV).
Nutzt die moderne REST-API, die GeoJSON direkt zurückgibt.

Collections (Stand März 2026):
  - Flurstueck, GebaeudeBauwerk, KatasterBezirk, Nutzung,
    NutzungFlurstueck, VerwaltungsEinheit

Attributnamen sind identisch zum LGLN-Schema (Kurznamen):
  flstkennz, gemarkung, gemaschl, flstnrzae, flstnrnen,
  flaeche, lagebeztxt, tntxt, kreis, gemeinde, aktualit

Besonderheit Hamburg: Keine Flurnummern — flurschl enthält nur
Gemarkungsschlüssel + Unterstriche (z.B. "020530___").
"""

import re
import requests
from typing import Optional, List
from wfs_clients import WFSClient, FlurstueckInfo
from coordinates import make_bbox_utm32

# Hamburg OGC API Features Endpunkte (korrekte Collection-Namen)
OAF_BASE_URL = "https://api.hamburg.de/datasets/v1/alkis_vereinfacht"
OAF_FLURSTUECK_URL = f"{OAF_BASE_URL}/collections/Flurstueck/items"
OAF_GEBAEUDE_URL = f"{OAF_BASE_URL}/collections/GebaeudeBauwerk/items"

HEADERS = {
    "User-Agent": "KatasterLookup/1.0",
    "Accept": "application/geo+json",
}


class HamburgClient(WFSClient):
    """OGC API Features Client für Hamburg."""

    @property
    def bundesland_name(self) -> str:
        return "Hamburg"

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
            print(f"[HH] OAF-Fehler: {e}")
            single = self._fallback_wfs(lat, lon, adresse)
            return [single] if single else []
        except ValueError as e:
            print(f"[HH] JSON-Parse-Fehler: {e}")
            return []

        features = data.get("features", [])
        if not features:
            print(f"[HH] Keine Flurstücke gefunden bei {lat}, {lon}")
            single = self._fallback_wfs(lat, lon, adresse)
            return [single] if single else []

        print(f"[HH] {len(features)} Flurstück(e) in der Antwort")

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
        info.bundesland = "Hamburg"
        info.quelle = "LGV Hamburg, Open Data (Datenlizenz DE-BY 2.0)"

        # Attribute — LGLN-Kurznamen (identisch zu Niedersachsen)
        info.gemarkung = props.get("gemarkung")
        info.gemarkungsnummer = props.get("gemaschl")
        info.flurstueckskennzeichen = props.get("flstkennz")

        # Hamburg hat keine Flurnummern — Flurschlüssel enthält Unterstriche
        flurschl = props.get("flurschl", "")
        if flurschl and "___" not in flurschl:
            info.flur = flurschl[-3:].lstrip("0") or None
        else:
            info.flur = None  # Hamburg kennt keine Fluren

        # Flurstücksnummer
        zae = props.get("flstnrzae")
        info.flurstueck_zaehler = str(zae) if zae else None
        nen = props.get("flstnrnen")
        info.flurstueck_nenner = str(nen) if nen else None

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

        print(f"[HH]   -> {info.gemarkung} Flurstück {info.flurstueck_display} "
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

    def _fallback_wfs(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """Fallback auf klassischen WFS, falls OGC API nicht verfügbar."""
        print("[HH] Versuche WFS-Fallback...")
        wfs_url = "https://geodienste.hamburg.de/WFS_HH_ALKIS_vereinfacht"
        bbox = make_bbox_utm32(lon, lat, buffer_m=25.0)
        min_east, min_north, max_east, max_north = bbox

        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": "ave:Flurstueck",
            "COUNT": "10",
            "BBOX": f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832",
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

        try:
            response = requests.get(
                wfs_url,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=30,
            )
            response.raise_for_status()
            from lxml import etree
            root = etree.fromstring(response.content)
            members = root.findall(
                ".//{http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0}Flurstueck"
            )
            if not members:
                print("[HH] WFS-Fallback: Keine Flurstücke")
                return None

            results = []
            for flst in members:
                info = FlurstueckInfo()
                info.bundesland = "Hamburg"
                info.quelle = "LGV Hamburg, Open Data (Datenlizenz DE-BY 2.0)"
                info.gemarkung = self._find_text(flst, "gemarkung")
                info.gemarkungsnummer = self._find_text(flst, "gemaschl")
                info.flurstueckskennzeichen = self._find_text(flst, "flstkennz")
                info.flurstueck_zaehler = self._find_text(flst, "flstnrzae")
                info.flurstueck_nenner = self._find_text(flst, "flstnrnen")
                info.lagebezeichnung = self._find_text(flst, "lagebeztxt")
                info.gemeinde = self._find_text(flst, "gemeinde")
                info.kreis = self._find_text(flst, "kreis")
                info.nutzungsart = self._find_text(flst, "tntxt")
                info.aktualitaet = self._find_text(flst, "aktualit")
                flaeche_str = self._find_text(flst, "flaeche")
                if flaeche_str:
                    try:
                        info.amtliche_flaeche = float(flaeche_str)
                    except ValueError:
                        pass
                results.append(info)

            if len(results) == 1:
                return results[0]
            if adresse:
                best = self._match_by_address(results, adresse)
                if best:
                    return best
            return results[0]

        except Exception as e:
            print(f"[HH] WFS-Fallback fehlgeschlagen: {e}")
            return None

    @staticmethod
    def _match_by_address(results: List[FlurstueckInfo], adresse: str) -> Optional[FlurstueckInfo]:
        """Findet das beste Match über die Lagebezeichnung."""
        match = re.search(r'(\d+)\s*([a-zA-Z])?', adresse)
        if not match:
            return None

        input_number = match.group(1)
        input_suffix = (match.group(2) or "").upper()
        input_full = f"{input_number}{input_suffix}".strip()

        print(f"[HH] Adress-Matching: suche '{input_full}' in {len(results)} Flurstücken")

        best_match = None

        for info in results:
            if not info.lagebezeichnung:
                continue
            lage_numbers = re.findall(r'(\d+)\s*([a-zA-Z])?(?:\s|$|,)', info.lagebezeichnung)
            for num, suffix in lage_numbers:
                lage_full = f"{num}{suffix.upper()}".strip()
                if lage_full == input_full:
                    print(f"[HH]   Exakter Treffer: '{info.lagebezeichnung}' -> {info.flurstueck_display}")
                    return info
                elif num == input_number and not best_match:
                    best_match = info

        return best_match

    @staticmethod
    def _find_text(element, local_name: str) -> Optional[str]:
        """Sucht Text eines Elements anhand des lokalen Namens."""
        for el in element.iter():
            tag_local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag_local.lower() == local_name.lower() and el.text:
                return el.text.strip()
        return None
