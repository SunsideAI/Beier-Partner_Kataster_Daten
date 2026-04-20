"""
WFS-Client für Bremen (Landesamt GeoInformation Bremen).
Bremen-Daten liegen auf dem LGLN-Server unter dem VOLLEN ALKIS-Schema
(adv:AX_Flurstueck), nicht dem vereinfachten Schema (ave:Flurstueck).
"""

import re
import requests
from lxml import etree
from typing import Optional, List
from wfs_clients import WFSClient, FlurstueckInfo
from coordinates import make_bbox_utm32

# LGLN-Endpunkt für Bremen (volles ALKIS-Schema)
WFS_BREMEN_LGLN_URL = "https://opendata.lgln.niedersachsen.de/doorman/noauth/alkishb_wfs_sf"


class BremenClient(WFSClient):
    """WFS-Client für Bremen über den LGLN-Dienst (volles ALKIS-Schema)."""

    @property
    def bundesland_name(self) -> str:
        return "Bremen"

    def query_flurstueck(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """Sucht das beste einzelne Flurstück."""
        results = self.query_flurstuecke(lat, lon, adresse)
        return results[0] if results else None

    def query_flurstuecke(self, lat: float, lon: float, adresse: str = "") -> List[FlurstueckInfo]:
        """Sucht ALLE Flurstücke in Bremen."""
        bbox = make_bbox_utm32(lon, lat, buffer_m=25.0)
        min_east, min_north, max_east, max_north = bbox

        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": "adv:AX_Flurstueck",
            "COUNT": "10",
            "BBOX": f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832",
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

        try:
            response = requests.get(
                WFS_BREMEN_LGLN_URL,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[HB] WFS-Fehler: {e}")
            return []

        results = self._parse_ax_flurstueck(response.content)
        if not results:
            print("[HB] Keine Flurstücke gefunden")
            return []

        for info in results:
            info.bundesland = "Bremen"
            info.quelle = "Landesamt GeoInformation Bremen (via LGLN), Open Data (CC BY 4.0)"

        # Bremen hat keine Lagebezeichnung → filter_by_address gibt alle zurück
        if adresse:
            return self.filter_by_address(results, adresse)

        return results

    def _parse_ax_flurstueck(self, xml_content: bytes) -> List[FlurstueckInfo]:
        """
        Parst die Antwort im vollen ALKIS-Schema (adv:AX_Flurstueck).
        Die Attribute heißen hier anders als im vereinfachten Schema.
        """
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as e:
            print(f"[HB] XML-Parse-Fehler: {e}")
            return []

        # Prüfe auf Exception
        if "ExceptionReport" in root.tag:
            exc = self._find_text(root, "ExceptionText")
            print(f"[HB] WFS-Exception: {exc}")
            return []

        # AX_Flurstueck suchen
        members = root.findall(
            ".//{http://www.adv-online.de/namespaces/adv/gid/6.0}AX_Flurstueck"
        )
        if not members:
            # Fallback: namespace-unabhängig
            for el in root.iter():
                local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if local == "AX_Flurstueck":
                    members.append(el)

        if not members:
            print(f"[HB] Keine AX_Flurstueck in der Antwort")
            # Debug
            print(f"[HB] Root: {root.tag}")
            returned = root.get("numberReturned", "?")
            print(f"[HB] numberReturned: {returned}")
            # Zeige erste Child-Tags
            for child in list(root)[:5]:
                print(f"[HB]   → {child.tag}")
                for sub in list(child)[:3]:
                    print(f"[HB]       → {sub.tag}")
            return []

        print(f"[HB] {len(members)} AX_Flurstueck in der Antwort")
        results = []

        for flst in members:
            info = self._parse_single_ax(flst)
            if info:
                results.append(info)

        return results

    def _parse_single_ax(self, flst) -> Optional[FlurstueckInfo]:
        """Parst ein einzelnes AX_Flurstueck (volles ALKIS-Schema)."""
        info = FlurstueckInfo()

        # Im vollen Schema heißen die Attribute:
        #   flurstueckskennzeichen, gemarkungsnummer, flurnummer,
        #   zaehler, nenner, amtlicheFlaeche
        # Die sind im adv-Namespace

        info.flurstueckskennzeichen = self._find_text(flst, "flurstueckskennzeichen")
        info.flur = self._find_text(flst, "flurnummer")
        info.flurstueck_zaehler = self._find_text(flst, "zaehler")
        info.flurstueck_nenner = self._find_text(flst, "nenner")

        # Fläche
        flaeche_str = self._find_text(flst, "amtlicheFlaeche")
        if flaeche_str:
            try:
                info.amtliche_flaeche = float(flaeche_str)
            except ValueError:
                pass

        # Flurnummer bereinigen
        if info.flur:
            info.flur = info.flur.lstrip("0") or "0"

        # Gemarkung: Im vollen Schema ist das eine Referenz — wir parsen
        # sie aus dem Flurstückskennzeichen
        if info.flurstueckskennzeichen:
            kz = info.flurstueckskennzeichen
            if len(kz) >= 6:
                info.gemarkungsnummer = kz[2:6]
            # Gemarkungsname muss ggf. separat ermittelt werden
            # Wir versuchen es aus dem gemarkung-Element
            gem_name = self._find_text(flst, "gemarkungsnummer")
            if not gem_name:
                gem_name = self._find_text(flst, "bezeichnung")
            # Gemarkungsname kann auch in einem verschachtelten Element stecken

        # Gemeinde aus zuständigeStelle oder Kennzeichen
        info.gemeinde = self._find_text(flst, "gemeinde")

        print(f"[HB]   → Gem. {info.gemarkungsnummer} Flur {info.flur} "
              f"Flurstück {info.flurstueck_display} ({info.flaeche_display})")
        return info

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
                lage_full = f"{num}{suffix.upper()}".strip()
                if lage_full == input_full:
                    return info
        return None

    @staticmethod
    def _find_text(element, local_name: str) -> Optional[str]:
        """Sucht Text anhand des lokalen Namens (namespace-unabhängig)."""
        for el in element.iter():
            tag_local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag_local.lower() == local_name.lower() and el.text:
                return el.text.strip()
        return None

    def query_gebaeude(self, lat: float, lon: float) -> Optional[float]:
        """Gebäudegrundfläche für Bremen über AX_Gebaeude."""
        bbox = make_bbox_utm32(lon, lat, buffer_m=10.0)
        min_east, min_north, max_east, max_north = bbox

        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": "adv:AX_Gebaeude",
            "COUNT": "20",
            "BBOX": f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832",
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

        try:
            response = requests.get(
                WFS_BREMEN_LGLN_URL,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException:
            return None

        try:
            root = etree.fromstring(response.content)
        except etree.XMLSyntaxError:
            return None

        total = 0.0
        for el in root.iter():
            local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if local == "grundflaeche" and el.text:
                try:
                    total += float(el.text.strip())
                except ValueError:
                    pass
        return total if total > 0 else None
