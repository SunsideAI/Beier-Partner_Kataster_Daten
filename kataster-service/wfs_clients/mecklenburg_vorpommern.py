"""
WFS-Client für Mecklenburg-Vorpommern (LAiV M-V).

Wie bei SH: Der vereinfachte WFS (alkis_wfs_einfach) hat ave:Flurstueck
in den Capabilities, akzeptiert aber keine BBOX-Abfragen.
Daher nutzen wir den INSPIRE-WFS mit CadastralParcel + CadastralZoning.
"""

import re
import requests
from lxml import etree
from typing import Optional, List, Dict
from wfs_clients import WFSClient, FlurstueckInfo
from coordinates import make_bbox_utm32

WFS_INSPIRE_URL = "https://www.geodaten-mv.de/dienste/inspire_cp_alkis_download"


class MecklenburgVorpommernClient(WFSClient):
    """WFS-Client für Mecklenburg-Vorpommern (INSPIRE-WFS)."""

    @property
    def bundesland_name(self) -> str:
        return "Mecklenburg-Vorpommern"

    def query_flurstueck(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """Sucht das Flurstück über den INSPIRE-WFS per BBOX."""
        bbox = make_bbox_utm32(lon, lat, buffer_m=25.0)
        min_east, min_north, max_east, max_north = bbox

        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": "cp:CadastralParcel",
            "COUNT": "10",
            "BBOX": f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832",
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

        try:
            response = requests.get(
                WFS_INSPIRE_URL,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[MV] INSPIRE-WFS-Fehler: {e}")
            return None

        results = self._parse_inspire_response(response.content)
        if not results:
            print("[MV] Keine Flurstücke gefunden")
            return None

        # Gemarkungsnamen per CadastralZoning nachschlagen
        gemarkung_names = self._lookup_gemarkung_names(min_east, min_north, max_east, max_north)
        for info in results:
            if info.gemarkungsnummer and info.gemarkungsnummer in gemarkung_names:
                info.gemarkung = gemarkung_names[info.gemarkungsnummer]

        if len(results) == 1:
            return results[0]

        if adresse:
            best = self._match_by_address(results, adresse)
            if best:
                return best

        return results[0]

    def _parse_inspire_response(self, xml_content: bytes) -> List[FlurstueckInfo]:
        """Parst die INSPIRE CadastralParcel-Antwort."""
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as e:
            print(f"[MV] XML-Parse-Fehler: {e}")
            return []

        parcels = root.findall(
            ".//{http://inspire.ec.europa.eu/schemas/cp/4.0}CadastralParcel"
        )
        if not parcels:
            return []

        print(f"[MV] {len(parcels)} Flurstück(e) in der INSPIRE-Antwort")
        results = []
        for parcel in parcels:
            info = self._parse_single_parcel(parcel)
            if info:
                results.append(info)
        return results

    def _parse_single_parcel(self, parcel) -> Optional[FlurstueckInfo]:
        """Parst ein einzelnes CadastralParcel-Element."""
        info = FlurstueckInfo()
        info.bundesland = "Mecklenburg-Vorpommern"
        info.quelle = "LAiV M-V, INSPIRE (CC BY 4.0)"

        # Kennzeichen aus gml:id
        gml_id = parcel.get("{http://www.opengis.net/gml/3.2}id", "")
        if "CadastralParcel_" in gml_id:
            info.flurstueckskennzeichen = gml_id.split("CadastralParcel_")[-1]
        else:
            identifier = self._find_text(parcel, "identifier")
            if identifier and "CadastralParcel_" in identifier:
                info.flurstueckskennzeichen = identifier.split("CadastralParcel_")[-1]

        # Fläche
        area_str = self._find_text(parcel, "areaValue")
        if area_str:
            try:
                info.amtliche_flaeche = float(area_str)
            except ValueError:
                pass

        # Aktualität
        info.aktualitaet = self._find_text(parcel, "beginLifespanVersion")

        # Kennzeichen zerlegen
        if info.flurstueckskennzeichen:
            self._parse_kennzeichen(info)

        print(f"[MV]   -> Gem. {info.gemarkungsnummer} Flur {info.flur} "
              f"Flurstück {info.flurstueck_display} ({info.flaeche_display})")
        return info

    @staticmethod
    def _parse_kennzeichen(info: FlurstueckInfo):
        """
        Zerlegt das Flurstückskennzeichen.
        Format: LLGGGGFFFZZZZZNNNNN (20 Zeichen, mit _ aufgefüllt)
        Beispiel: 130332001026360058__ -> Land=13, Gem=0332, Flur=001, Z=02636, N=0058
        """
        kz = info.flurstueckskennzeichen
        if not kz or len(kz) < 18:
            return

        clean = kz.replace("_", "0")

        try:
            info.gemarkungsnummer = kz[2:6]

            flur_raw = clean[6:9]
            info.flur = flur_raw.lstrip("0") or "0"

            zaehler_raw = clean[9:14]
            info.flurstueck_zaehler = zaehler_raw.lstrip("0") or "0"

            nenner_raw = kz[14:19]
            nenner_clean = nenner_raw.replace("_", "").lstrip("0")
            info.flurstueck_nenner = nenner_clean if nenner_clean else None
        except (IndexError, ValueError):
            pass

    def _lookup_gemarkung_names(
        self, min_east: float, min_north: float, max_east: float, max_north: float
    ) -> Dict[str, str]:
        """Fragt CadastralZoning per BBOX ab für Gemarkungsnamen."""
        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": "cp:CadastralZoning",
            "COUNT": "10",
            "BBOX": f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832",
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

        try:
            response = requests.get(
                WFS_INSPIRE_URL,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[MV] CadastralZoning-Abfrage fehlgeschlagen: {e}")
            return {}

        try:
            root = etree.fromstring(response.content)
        except etree.XMLSyntaxError:
            return {}

        result = {}
        zonings = root.findall(
            ".//{http://inspire.ec.europa.eu/schemas/cp/4.0}CadastralZoning"
        )

        for zoning in zonings:
            # Nur Gemarkungsebene (levelName == "Gemarkung")
            level_name = None
            for el in zoning.iter():
                tag_local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if tag_local == "LocalisedCharacterString" and el.text:
                    level_name = el.text.strip()
                    break

            if level_name != "Gemarkung":
                continue

            # Gemarkungsnummer aus gml:id
            gml_id = zoning.get("{http://www.opengis.net/gml/3.2}id", "")
            if "CadastralZoning_" not in gml_id:
                continue
            raw = gml_id.split("CadastralZoning_")[-1]
            gem_nr = raw[2:6] if len(raw) >= 6 else None
            if not gem_nr:
                continue

            # Name aus gn:SpellingOfName/gn:text
            name = None
            ns_gn = "http://inspire.ec.europa.eu/schemas/gn/4.0"
            for spelling in zoning.findall(f".//{{{ns_gn}}}SpellingOfName"):
                text_el = spelling.find(f"{{{ns_gn}}}text")
                if text_el is not None and text_el.text:
                    candidate = text_el.text.strip()
                    if not candidate.isdigit():
                        name = candidate
                        break

            if gem_nr and name:
                result[gem_nr] = name
                print(f"[MV] Gemarkung {gem_nr} = {name}")

        return result

    @staticmethod
    def _match_by_address(results: List[FlurstueckInfo], adresse: str) -> Optional[FlurstueckInfo]:
        """Adress-Matching über Lagebezeichnung."""
        match = re.search(r'(\d+)\s*([a-zA-Z])?', adresse)
        if not match:
            return None
        input_number = match.group(1)
        input_suffix = (match.group(2) or "").upper()
        input_full = f"{input_number}{input_suffix}".strip()

        for info in results:
            if not info.lagebezeichnung:
                continue
            lage_numbers = re.findall(r'(\d+)\s*([a-zA-Z])?(?:\s|$|,)', info.lagebezeichnung)
            for num, suffix in lage_numbers:
                if f"{num}{suffix.upper()}".strip() == input_full:
                    return info
        return None

    @staticmethod
    def _find_text(element, local_name: str) -> Optional[str]:
        """Sucht Text anhand des lokalen Namens."""
        for el in element.iter():
            tag_local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag_local.lower() == local_name.lower() and el.text:
                return el.text.strip()
        return None
