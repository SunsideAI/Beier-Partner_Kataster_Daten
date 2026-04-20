"""
WFS-Client für Niedersachsen (LGLN).
Endpunkt: ALKIS WFS vereinfacht (einfach)
"""

import logging
import requests
from lxml import etree
from typing import Optional, List
from wfs_clients import WFSClient, FlurstueckInfo
from coordinates import make_bbox_utm32

logger = logging.getLogger(__name__)

# Niedersachsen ALKIS WFS Endpunkte
WFS_FLURSTUECK_URL = "https://opendata.lgln.niedersachsen.de/doorman/noauth/alkis_wfs_einfach"

# Namespaces im vereinfachten ALKIS-Schema
NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "ave": "http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "fes": "http://www.opengis.net/fes/2.0",
}

# WFS GetFeature Request als XML Template
GET_FEATURE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature
    service="WFS"
    version="2.0.0"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:ave="http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0"
    count="5">
  <wfs:Query typeNames="ave:Flurstueck">
    <fes:Filter>
      <fes:BBOX>
        <fes:ValueReference>position</fes:ValueReference>
        <gml:Envelope srsName="urn:ogc:def:crs:EPSG::25832">
          <gml:lowerCorner>{min_east} {min_north}</gml:lowerCorner>
          <gml:upperCorner>{max_east} {max_north}</gml:upperCorner>
        </gml:Envelope>
      </fes:BBOX>
    </fes:Filter>
  </wfs:Query>
</wfs:GetFeature>"""

# Alternativ: KVP-basierte Abfrage (einfacher, als Fallback)
GET_FEATURE_KVP_PARAMS = {
    "SERVICE": "WFS",
    "VERSION": "2.0.0",
    "REQUEST": "GetFeature",
    "TYPENAMES": "ave:Flurstueck",
    "COUNT": "5",
    "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
}

# Gebäude-Abfrage
GET_GEBAEUDE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature
    service="WFS"
    version="2.0.0"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:ave="http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0"
    count="20">
  <wfs:Query typeNames="ave:Gebaeude">
    <fes:Filter>
      <fes:BBOX>
        <fes:ValueReference>position</fes:ValueReference>
        <gml:Envelope srsName="urn:ogc:def:crs:EPSG::25832">
          <gml:lowerCorner>{min_east} {min_north}</gml:lowerCorner>
          <gml:upperCorner>{max_east} {max_north}</gml:upperCorner>
        </gml:Envelope>
      </fes:BBOX>
    </fes:Filter>
  </wfs:Query>
</wfs:GetFeature>"""

HEADERS = {
    "Content-Type": "application/xml",
    "User-Agent": "KatasterLookup/1.0",
}


class NiedersachsenClient(WFSClient):
    """WFS-Client für Niedersachsen (LGLN Open Data)."""

    @property
    def bundesland_name(self) -> str:
        return "Niedersachsen"

    def query_flurstueck(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """Sucht das beste einzelne Flurstück (Abwärtskompatibilität)."""
        results = self.query_flurstuecke(lat, lon, adresse)
        return results[0] if results else None

    def query_flurstuecke(self, lat: float, lon: float, adresse: str = "") -> List[FlurstueckInfo]:
        """Sucht ALLE passenden Flurstücke an den gegebenen WGS84-Koordinaten."""
        bbox = make_bbox_utm32(lon, lat, buffer_m=25.0)

        # Versuch 1: KVP GET Request (zuverlässiger)
        results = self._query_kvp_all(bbox)
        if not results:
            # Versuch 2: XML POST Request
            results = self._query_xml_all(bbox)

        if not results:
            return []

        # Nach Lagebezeichnung filtern
        if adresse:
            return self.filter_by_address(results, adresse)

        return [results[0]]

    def _query_kvp_all(self, bbox: tuple) -> List[FlurstueckInfo]:
        """WFS-Abfrage per KVP GET — gibt alle Treffer zurück."""
        min_east, min_north, max_east, max_north = bbox
        params = GET_FEATURE_KVP_PARAMS.copy()
        params["BBOX"] = f"{min_east},{min_north},{max_east},{max_north},urn:ogc:def:crs:EPSG::25832"
        params["COUNT"] = "10"

        try:
            response = requests.get(
                WFS_FLURSTUECK_URL,
                params=params,
                headers={"User-Agent": "KatasterLookup/1.0"},
                timeout=30,
            )
            response.raise_for_status()
            return self._parse_all_flurstuecke(response.content)
        except requests.RequestException as e:
            logger.warning("[NI] WFS KVP-Fehler: %s", e)
            return []

    def _query_xml_all(self, bbox: tuple) -> List[FlurstueckInfo]:
        """WFS-Abfrage per XML POST — gibt alle Treffer zurück."""
        min_east, min_north, max_east, max_north = bbox
        xml_body = GET_FEATURE_TEMPLATE.format(
            min_east=min_east,
            min_north=min_north,
            max_east=max_east,
            max_north=max_north,
        )

        try:
            response = requests.post(
                WFS_FLURSTUECK_URL,
                data=xml_body.encode("utf-8"),
                headers=HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            return self._parse_all_flurstuecke(response.content)
        except requests.RequestException as e:
            logger.warning("[NI] WFS XML-Fehler: %s", e)
            return []

    @staticmethod
    def _match_by_address(results: List[FlurstueckInfo], adresse: str) -> Optional[FlurstueckInfo]:
        """
        Findet aus mehreren Flurstücken das beste Match anhand der Lagebezeichnung.
        Extrahiert die Hausnummer (inkl. Zusatz) und vergleicht sie.
        """
        import re

        # Hausnummer + Zusatz aus der Eingabeadresse extrahieren
        # Erkennt: "101A", "101 A", "101a", "101 a"
        match = re.search(r'(\d+)\s*([a-zA-Z])?', adresse)
        if not match:
            return None

        input_number = match.group(1)
        input_suffix = (match.group(2) or "").upper()
        input_full = f"{input_number}{input_suffix}".strip()

        logger.info("[NI] Adress-Matching: suche Hausnummer '%s' in %d Flurstuecken", input_full, len(results))

        best_match = None
        best_score = -1

        for info in results:
            if not info.lagebezeichnung:
                continue

            lage = info.lagebezeichnung
            # Hausnummern aus der Lagebezeichnung extrahieren
            # ALKIS-Format: "Schölischer Straße (Schölisch) 101A" oder "101 A"
            lage_numbers = re.findall(r'(\d+)\s*([a-zA-Z])?(?:\s|$|,)', lage)

            for num, suffix in lage_numbers:
                lage_full = f"{num}{suffix.upper()}".strip()

                if lage_full == input_full:
                    # Exakter Treffer (Hausnummer + Zusatz)
                    logger.info("[NI]   Exakter Treffer: '%s' → %s", lage, info.flurstueck_display)
                    return info
                elif num == input_number and best_score < 1:
                    # Hausnummer stimmt, Zusatz nicht — zweitbestes Match
                    best_match = info
                    best_score = 1
                    logger.info("[NI]   Teilmatch (nur Nummer): '%s' → %s", lage, info.flurstueck_display)

        return best_match

    def _parse_all_flurstuecke(self, xml_content: bytes) -> List[FlurstueckInfo]:
        """Parst die WFS-Antwort und extrahiert ALLE Flurstücke."""
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as e:
            logger.warning("[NI] XML-Parse-Fehler: %s", e)
            return []

        # Prüfe auf ExceptionReport
        if "ExceptionReport" in root.tag or root.find(".//{http://www.opengis.net/ows/1.1}ExceptionReport") is not None:
            exc_text = root.find(".//{http://www.opengis.net/ows/1.1}ExceptionText")
            msg = exc_text.text if exc_text is not None else "Unbekannter WFS-Fehler"
            logger.warning("[NI] WFS-Exception: %s", msg)
            return []

        # Suche nach Flurstück-Elementen
        members = root.findall(".//{http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0}Flurstueck")
        if not members:
            members = root.findall(".//ave:Flurstueck", NS)

        if not members:
            logger.info("[NI] Keine Flurstücke in der Antwort gefunden.")
            return []

        logger.info("[NI] %d Flurstück(e) in der Antwort", len(members))
        results = []

        for flst in members:
            info = self._parse_single_flurstueck(flst)
            if info:
                results.append(info)

        return results

    def _parse_single_flurstueck(self, flst) -> Optional[FlurstueckInfo]:
        """Parst ein einzelnes Flurstück-XML-Element."""
        info = FlurstueckInfo()
        info.bundesland = "Niedersachsen"
        info.quelle = "LGLN Niedersachsen, Open Data"

        # Attribute extrahieren — LGLN verwendet Kurznamen:
        #   gemarkung, gemaschl, flur, flstnrzae, flstnrnen,
        #   flstkennz, flaeche, lagebeztxt, tntxt, kreis, gemeinde
        info.gemarkung = self._get_text(flst, [
            "gemarkung", "ave:gemarkung",
        ])
        info.gemarkungsnummer = self._get_text(flst, [
            "gemaschl", "ave:gemaschl",
        ])
        info.flur = self._get_text(flst, [
            "flur", "ave:flur",
        ])
        info.flurstueck_zaehler = self._get_text(flst, [
            "flstnrzae", "ave:flstnrzae",
        ])
        info.flurstueck_nenner = self._get_text(flst, [
            "flstnrnen", "ave:flstnrnen",
        ])
        info.flurstueckskennzeichen = self._get_text(flst, [
            "flstkennz", "ave:flstkennz",
        ])

        # Fläche
        flaeche_str = self._get_text(flst, [
            "flaeche", "ave:flaeche",
        ])
        if flaeche_str:
            try:
                info.amtliche_flaeche = float(flaeche_str)
            except ValueError:
                pass

        # Flurnummer: führende Nullen entfernen (z.B. "007" → "7")
        if info.flur:
            info.flur = info.flur.lstrip("0") or "0"

        # Zusatzinfos (LGLN-spezifisch)
        info.lagebezeichnung = self._get_text(flst, [
            "lagebeztxt", "ave:lagebeztxt",
        ])
        info.gemeinde = self._get_text(flst, [
            "gemeinde", "ave:gemeinde",
        ])
        info.kreis = self._get_text(flst, [
            "kreis", "ave:kreis",
        ])
        info.nutzungsart = self._get_text(flst, [
            "tntxt", "ave:tntxt",
        ])
        info.aktualitaet = self._get_text(flst, [
            "aktualit", "ave:aktualit",
        ])

        logger.info("[NI]   → %s Flur %s Flurstück %s (%s) Lage: %s",
                    info.gemarkung, info.flur, info.flurstueck_display,
                    info.flaeche_display, info.lagebezeichnung)
        return info

    def query_gebaeude(self, lat: float, lon: float) -> Optional[float]:
        """Ermittelt die Gebäudegrundfläche am Standort.

        Bekannte Einschränkung: ave:Gebaeude ist im LGLN vereinfacht-Schema
        enthalten, aber der Dienst liefert abhängig von der BBOX-Größe und
        dem genauen Geocoding-Punkt nicht immer ein Ergebnis. Bei null-Return
        ist das Feld gebaeude_grundflaeche_qm im Response null.
        """
        # 30m Buffer: Gebäude-Polygon liegt häufig versetzt zum Adresspunkt
        # (Nominatim geocodiert oft an die Straßenseite, nicht die Gebäudemitte)
        bbox = make_bbox_utm32(lon, lat, buffer_m=30.0)
        min_east, min_north, max_east, max_north = bbox

        xml_body = GET_GEBAEUDE_TEMPLATE.format(
            min_east=min_east,
            min_north=min_north,
            max_east=max_east,
            max_north=max_north,
        )

        logger.debug("[NI] Gebaeude-BBOX utm32: %.1f %.1f %.1f %.1f", *bbox)

        try:
            response = requests.post(
                WFS_FLURSTUECK_URL,
                data=xml_body.encode("utf-8"),
                headers=HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            logger.debug("[NI] Gebaeude-Response (%d bytes): %s",
                         len(response.content), response.content[:400])
            result = self._parse_gebaeude_flaeche(response.content)
            if result is None:
                logger.info("[NI] Gebaeude: kein Treffer in BBOX (ave:Gebaeude liefert 0 Members)")
            return result
        except requests.RequestException as e:
            logger.warning("[NI] Gebaeude-Abfrage-Fehler: %s", e)
            return None

    def _parse_gebaeude_flaeche(self, xml_content: bytes) -> Optional[float]:
        """Berechnet die Summe der Gebäudegrundflächen aus der WFS-Antwort."""
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError:
            return None

        gebaeude_list = root.findall(
            ".//{http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0}Gebaeude"
        )
        if not gebaeude_list:
            return None

        total_area = 0.0
        for geb in gebaeude_list:
            # Grundfläche aus Attribut
            flaeche_str = self._get_text(geb, [
                "ave:grundflaeche", "grundflaeche",
                "ave:gebaeudegrundflaeche", "gebaeudegrundflaeche",
            ])
            if flaeche_str:
                try:
                    total_area += float(flaeche_str)
                except ValueError:
                    pass

        return total_area if total_area > 0 else None

    def _get_text(self, element, tag_names: list) -> Optional[str]:
        """Sucht in einem XML-Element nach verschiedenen Tag-Namen."""
        for tag in tag_names:
            if ":" in tag:
                # Namespace-qualifiziert
                el = element.find(tag, NS)
            else:
                # Ohne Namespace
                el = element.find(tag)
            if el is not None and el.text:
                return el.text.strip()

        # Letzter Versuch: Namespace-unabhängig mit local-name
        for tag in tag_names:
            local_name = tag.split(":")[-1] if ":" in tag else tag
            for child in element:
                child_local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child_local.lower() == local_name.lower() and child.text:
                    return child.text.strip()
        return None

    @staticmethod
    def _parse_gemarkung_from_kennzeichen(kennzeichen: str) -> Optional[str]:
        """
        Versucht, die Gemarkungsnummer aus dem Flurstückskennzeichen zu extrahieren.
        Format: LLGGGG-FFF-ZZZZZ/NNN (Land-Gemarkung-Flur-Zähler/Nenner)
        """
        # Das Kennzeichen enthält die Gemarkungsnummer, aber nicht den Namen
        # Wir geben hier nur die Nummer zurück, falls kein Name vorhanden
        if kennzeichen and len(kennzeichen) >= 6:
            return kennzeichen[2:6]  # Gemarkungsnummer (4-stellig)
        return None
