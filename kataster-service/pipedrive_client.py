"""
Minimaler Pipedrive API Client.
Schreibt Katasterdaten in benutzerdefinierte Felder eines Deals.

Konfiguration via Umgebungsvariablen:
  PIPEDRIVE_API_TOKEN       API-Token aus den Pipedrive-Einstellungen
  PIPEDRIVE_COMPANY_DOMAIN  z.B. "beierpartner" (für beierpartner.pipedrive.com)
  PIPEDRIVE_FIELD_MAP       JSON-String: {"kataster_key": "pipedrive_field_key", ...}

Beispiel PIPEDRIVE_FIELD_MAP:
  {"gemarkung":"abc12def","flur":"xyz34ghi","flurstueck":"jkl56mno",
   "amtliche_flaeche_qm":"pqr78stu","lagebezeichnung":"vwx90yza","bundesland":"bcd12efg"}
"""

import os
import json
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

_API_TOKEN = os.environ.get("PIPEDRIVE_API_TOKEN", "")
_DOMAIN = os.environ.get("PIPEDRIVE_COMPANY_DOMAIN", "")

try:
    FIELD_MAP: dict = json.loads(os.environ.get("PIPEDRIVE_FIELD_MAP", "{}"))
except json.JSONDecodeError:
    logger.warning("PIPEDRIVE_FIELD_MAP ist kein gültiges JSON — leeres Mapping verwendet")
    FIELD_MAP = {}


def _base_url() -> str:
    if not _DOMAIN:
        raise ValueError("PIPEDRIVE_COMPANY_DOMAIN Env-Variable nicht gesetzt")
    return f"https://{_DOMAIN}.pipedrive.com/api/v1"


def get_deal(deal_id: int) -> Optional[dict]:
    """
    Holt einen Deal aus Pipedrive.
    Kann genutzt werden um zu prüfen, ob Felder bereits gesetzt sind (Idempotenz).
    """
    if not _API_TOKEN or not _DOMAIN:
        logger.error("Pipedrive-Credentials nicht konfiguriert")
        return None
    try:
        response = requests.get(
            f"{_base_url()}/deals/{deal_id}",
            params={"api_token": _API_TOKEN},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data") if data.get("success") else None
    except requests.RequestException as e:
        logger.error("Pipedrive get_deal(%d) fehlgeschlagen: %s", deal_id, e)
        return None


def update_deal_fields(deal_id: int, kataster_data: dict) -> bool:
    """
    Schreibt Katasterdaten in die benutzerdefinierten Felder eines Deals.

    Args:
        deal_id:       Pipedrive Deal-ID
        kataster_data: dict aus FlurstueckInfo.to_dict() (ggf. ergänzt um bundesland)

    Returns:
        True bei Erfolg, False bei Fehler
    """
    if not _API_TOKEN or not _DOMAIN:
        logger.error("Pipedrive-Credentials nicht konfiguriert")
        return False

    if not FIELD_MAP:
        logger.warning("PIPEDRIVE_FIELD_MAP leer — keine Felder zu aktualisieren")
        return False

    payload = {
        pipedrive_key: kataster_data[kataster_key]
        for kataster_key, pipedrive_key in FIELD_MAP.items()
        if kataster_data.get(kataster_key) is not None
    }

    if not payload:
        logger.warning("Kein Kataster-Feld passt zum FIELD_MAP — Update übersprungen")
        return False

    try:
        response = requests.put(
            f"{_base_url()}/deals/{deal_id}",
            params={"api_token": _API_TOKEN},
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            logger.info("Deal %d aktualisiert (%d Felder)", deal_id, len(payload))
            return True
        logger.error("Pipedrive API Fehler: %s", data.get("error"))
        return False
    except requests.RequestException as e:
        logger.error("Pipedrive update_deal(%d) fehlgeschlagen: %s", deal_id, e)
        return False
