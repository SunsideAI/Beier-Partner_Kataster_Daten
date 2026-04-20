"""
Basis-Klasse für WFS-Abfragen an die ALKIS-Dienste der Bundesländer.
"""

from dataclasses import dataclass, field
from typing import Optional, List, List
from abc import ABC, abstractmethod


def _clean_nutzungsart(raw: Optional[str]) -> Optional[str]:
    """Entfernt angehängte Flächenangaben und ALKIS-Funktionscodes aus tntxt.
    Beispiele: "Wohnbaufläche;594" → "Wohnbaufläche"
               "Straßenverkehr(funktion:null);97" → "Straßenverkehr"
    """
    if not raw:
        return raw
    return raw.split(";")[0].split("(")[0].strip() or None


def _clean_aktualitaet(raw: Optional[str]) -> Optional[str]:
    """Entfernt das Z-Suffix von reinen Datumsangaben ohne Uhrzeit.
    Beispiel: "2025-08-07Z" → "2025-08-07"
              "2025-08-07T00:00:00Z" → unverändert (gültiges ISO-Datetime)
    """
    if not raw:
        return raw
    if "T" not in raw and raw.endswith("Z"):
        return raw[:-1]
    return raw


@dataclass
class FlurstueckInfo:
    """Katasterdaten eines Flurstücks."""
    gemarkung: Optional[str] = None
    gemarkungsnummer: Optional[str] = None
    flur: Optional[str] = None
    flurstueck_zaehler: Optional[str] = None
    flurstueck_nenner: Optional[str] = None
    flurstueckskennzeichen: Optional[str] = None
    amtliche_flaeche: Optional[float] = None  # in m²
    # Optional: Gebäudegrundfläche
    gebaeude_grundflaeche: Optional[float] = None  # in m²
    # Zusatzinfos (je nach Bundesland verfügbar)
    lagebezeichnung: Optional[str] = None
    gemeinde: Optional[str] = None
    kreis: Optional[str] = None
    nutzungsart: Optional[str] = None
    aktualitaet: Optional[str] = None
    # Metadaten
    quelle: Optional[str] = None
    bundesland: Optional[str] = None

    @property
    def flurstueck_display(self) -> str:
        """Flurstücks-Bezeichnung als String, z.B. '45/3' oder '45'."""
        if self.flurstueck_zaehler and self.flurstueck_nenner:
            return f"{self.flurstueck_zaehler}/{self.flurstueck_nenner}"
        elif self.flurstueck_zaehler:
            return self.flurstueck_zaehler
        return ""

    @property
    def flaeche_display(self) -> str:
        """Flurstücksfläche formatiert."""
        if self.amtliche_flaeche is not None:
            return f"{self.amtliche_flaeche:,.0f} m²".replace(",", ".")
        return "unbekannt"

    def to_dict(self) -> dict:
        """Konvertiert in ein Dictionary für die JSON-Ausgabe."""
        result = {
            "gemarkung": self.gemarkung,
            "gemarkungsnummer": self.gemarkungsnummer,
            "flur": self.flur,
            "flurstueck": self.flurstueck_display,
            "flurstueck_zaehler": self.flurstueck_zaehler,
            "flurstueck_nenner": self.flurstueck_nenner,
            "flurstueckskennzeichen": self.flurstueckskennzeichen,
            "amtliche_flaeche_qm": self.amtliche_flaeche,
            "gebaeude_grundflaeche_qm": self.gebaeude_grundflaeche,
            "lagebezeichnung": self.lagebezeichnung,
            "gemeinde": self.gemeinde,
            "kreis": self.kreis,
            "nutzungsart": _clean_nutzungsart(self.nutzungsart),
            "aktualitaet": _clean_aktualitaet(self.aktualitaet),
            "quelle": self.quelle,
            "bundesland": self.bundesland,
        }
        return result


class WFSClient(ABC):
    """Abstrakte Basis-Klasse für Bundesland-spezifische WFS-Clients."""

    @property
    @abstractmethod
    def bundesland_name(self) -> str:
        """Name des Bundeslands."""
        ...

    @abstractmethod
    def query_flurstueck(self, lat: float, lon: float, adresse: str = "") -> Optional[FlurstueckInfo]:
        """
        Sucht das Flurstück an den gegebenen Koordinaten.
        Gibt das beste einzelne Ergebnis zurück.
        """
        ...

    def query_flurstuecke(self, lat: float, lon: float, adresse: str = "") -> List[FlurstueckInfo]:
        """
        Sucht ALLE passenden Flurstücke an den gegebenen Koordinaten.
        Filtert nach Lagebezeichnung, falls vorhanden.
        
        Standardimplementierung: ruft query_flurstueck auf und gibt Liste zurück.
        Clients mit Multi-Ergebnis-Unterstützung überschreiben diese Methode.
        """
        result = self.query_flurstueck(lat, lon, adresse)
        return [result] if result else []

    def query_gebaeude(self, lat: float, lon: float) -> Optional[float]:
        """
        Optional: Ermittelt die Gebäudegrundfläche am Standort.
        Standardmäßig nicht implementiert.
        
        Returns:
            Grundfläche in m² oder None
        """
        return None

    @staticmethod
    def filter_by_address(results: List[FlurstueckInfo], adresse: str) -> List[FlurstueckInfo]:
        """
        Filtert eine Liste von Flurstücken nach passender Lagebezeichnung.
        Gibt ALLE Flurstücke zurück, deren Lagebezeichnung zur Adresse passt.
        
        Falls keine Lagebezeichnung vorhanden (INSPIRE-Dienste), wird die
        gesamte Liste zurückgegeben.
        """
        import re

        if not adresse or not results:
            return results

        # Hausnummer + Zusatz aus Adresse extrahieren
        match = re.search(r'(\d+)\s*([a-zA-Z])?', adresse)
        if not match:
            return results

        input_number = match.group(1)
        input_suffix = (match.group(2) or "").upper()
        input_full = f"{input_number}{input_suffix}".strip()

        # Prüfe ob überhaupt Lagebezeichnungen vorhanden sind
        has_lage = any(info.lagebezeichnung for info in results)
        if not has_lage:
            return results  # Ohne Lagebezeichnung können wir nicht filtern

        print(f"[Filter] Suche alle Flurstücke mit Hausnummer '{input_full}'")

        # Exakte Matches sammeln (Hausnummer + Zusatz)
        exact_matches = []
        # Nummer-Matches als Fallback (nur Hausnummer, ohne Zusatz)
        number_matches = []

        for info in results:
            if not info.lagebezeichnung:
                continue

            # NRW/NI: lagebeztxt kann ";" als Trenner haben
            lage_parts = info.lagebezeichnung.replace(";", " ")
            lage_numbers = re.findall(r'(\d+)\s*([a-zA-Z])?(?:\s|$|,|;)', lage_parts)

            for num, suffix in lage_numbers:
                lage_full = f"{num}{suffix.upper()}".strip()
                if lage_full == input_full:
                    exact_matches.append(info)
                    print(f"[Filter]   Exakt: '{info.lagebezeichnung}' → {info.flurstueck_display}")
                    break
                elif num == input_number and info not in number_matches:
                    number_matches.append(info)

        if exact_matches:
            return exact_matches
        elif number_matches:
            print(f"[Filter]   Kein exakter Treffer, {len(number_matches)} Nummer-Matches")
            return number_matches
        else:
            # Kein Match → erstes Ergebnis als Fallback
            print(f"[Filter]   Kein Match gefunden, verwende nächstliegendes Flurstück")
            return [results[0]]
