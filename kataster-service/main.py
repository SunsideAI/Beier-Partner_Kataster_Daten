"""
Kataster-Lookup-Service
=======================
REST-API zur Abfrage von Katasterdaten (Gemarkung, Flur, Flurstück, Fläche)
anhand einer Adresse. Unterstützt: Niedersachsen, Hamburg, Bremen,
Schleswig-Holstein, Mecklenburg-Vorpommern, Nordrhein-Westfalen.

Aufruf: GET /kataster?adresse=Musterstraße 1, 21680 Stade
"""

import os
import hmac
import json
import hashlib
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Security, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.security.api_key import APIKeyHeader

load_dotenv()  # no-op in production (Railway sets env vars directly)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Auth: X-API-Key (für GET /kataster)
# ──────────────────────────────────────────────

_VALID_KEYS: set[str] = {
    k.strip()
    for k in os.environ.get("API_KEYS", "").split(",")
    if k.strip()
}

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(_api_key_header)) -> str:
    """FastAPI dependency. Raises 401 for invalid/missing key.
    If API_KEYS env var is unset, auth is disabled (local dev convenience).
    Blocked at startup on Railway — see _startup_check().
    """
    if not _VALID_KEYS:
        return ""
    if not api_key or api_key not in _VALID_KEYS:
        raise HTTPException(
            status_code=401,
            detail={"fehler": "Ungültiger oder fehlender API-Key", "header": "X-API-Key"},
        )
    return api_key


# ──────────────────────────────────────────────
# Auth: HTTP Basic Auth (für POST /pipedrive/webhook)
#
# Pipedrive unterstützt keine Custom-Header bei Webhooks.
# Stattdessen: Basic Auth via URL-Credentials.
# Webhook-URL-Format: https://USER:PASSWORD@dein-service.railway.app/pipedrive/webhook
# ──────────────────────────────────────────────

_http_basic = HTTPBasic(auto_error=False)


async def verify_webhook_auth(
    credentials: Optional[HTTPBasicCredentials] = Depends(_http_basic),
) -> None:
    """HTTP Basic Auth für /pipedrive/webhook.
    Blocked at startup on Railway when credentials are missing.
    """
    webhook_user = os.environ.get("PIPEDRIVE_WEBHOOK_USER", "")
    webhook_password = os.environ.get("PIPEDRIVE_WEBHOOK_PASSWORD", "")

    if not webhook_user or not webhook_password:
        return  # not configured → open (only reaches here in local dev)

    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="HTTP Basic Auth erforderlich",
            headers={"WWW-Authenticate": "Basic"},
        )

    user_ok = secrets.compare_digest(credentials.username.encode(), webhook_user.encode())
    pass_ok = secrets.compare_digest(credentials.password.encode(), webhook_password.encode())

    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401,
            detail="Ungültige Webhook-Credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


def _verify_webhook_signature(headers, body_bytes: bytes) -> None:
    """Optionale zweite Verteidigungslinie: HMAC-SHA256 Signatur-Verifikation.
    Aktiv wenn PIPEDRIVE_WEBHOOK_SECRET gesetzt. Nur Warn-Log wenn Header fehlt.
    """
    secret = os.environ.get("PIPEDRIVE_WEBHOOK_SECRET", "")
    if not secret:
        return

    signature = headers.get("x-pipedrive-signature", "")
    if not signature:
        logger.warning(
            "Webhook: X-Pipedrive-Signature Header fehlt "
            "(PIPEDRIVE_WEBHOOK_SECRET konfiguriert — Pipedrive-Version prüfen)"
        )
        return

    expected = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise HTTPException(status_code=401, detail="Ungültige Webhook-Signatur")


# ──────────────────────────────────────────────
# Startup-Check: Fail fast auf Railway
# ──────────────────────────────────────────────

def _startup_check() -> None:
    """Prüft kritische Env-Variablen beim Start.
    Auf Railway (RAILWAY_ENVIRONMENT gesetzt): RuntimeError = Container-Crash mit Fehlermeldung.
    Lokal: nur Log-Ausgabe.
    """
    is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT"))

    api_keys_active = bool(_VALID_KEYS)
    webhook_user = os.environ.get("PIPEDRIVE_WEBHOOK_USER", "")
    webhook_password = os.environ.get("PIPEDRIVE_WEBHOOK_PASSWORD", "")
    webhook_auth_active = bool(webhook_user and webhook_password)
    webhook_sig_active = bool(os.environ.get("PIPEDRIVE_WEBHOOK_SECRET"))

    logger.info(
        "Auth-Status: API_KEYS=%s | Webhook Basic Auth=%s | Webhook HMAC-Signatur=%s",
        "aktiv" if api_keys_active else "DEAKTIVIERT",
        "aktiv" if webhook_auth_active else "DEAKTIVIERT",
        "aktiv" if webhook_sig_active else "deaktiviert (optional)",
    )

    if is_railway:
        errors = []
        if not api_keys_active:
            errors.append("API_KEYS fehlt — /kataster wäre offen für alle")
        if not webhook_auth_active:
            errors.append(
                "PIPEDRIVE_WEBHOOK_USER und/oder PIPEDRIVE_WEBHOOK_PASSWORD fehlt "
                "— /pipedrive/webhook wäre offen für alle"
            )
        if errors:
            for e in errors:
                logger.critical("STARTUP-FEHLER: %s", e)
            raise RuntimeError(
                "Kritische Env-Variablen fehlen: " + "; ".join(errors)
            )
        logger.info("Railway-Startup-Check: OK")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _startup_check()
    yield


# ──────────────────────────────────────────────
# Imports & App
# ──────────────────────────────────────────────

from geocoder import geocode, is_supported, SUPPORTED_STATES
from wfs_clients import FlurstueckInfo
from wfs_clients.niedersachsen import NiedersachsenClient
from wfs_clients.hamburg import HamburgClient
from wfs_clients.bremen import BremenClient
from wfs_clients.schleswig_holstein import SchleswigHolsteinClient
from wfs_clients.mecklenburg_vorpommern import MecklenburgVorpommernClient
from wfs_clients.nordrhein_westfalen import NordrheinWestfalenClient

app = FastAPI(
    title="Kataster-Lookup-Service",
    description=(
        "Ermittelt Katasterdaten (Gemarkung, Flur, Flurstück, Fläche) "
        "anhand einer Adresse über die Open-Data-Dienste der Landesvermessungsämter."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ──────────────────────────────────────────────
# WFS-Clients je Bundesland
# ──────────────────────────────────────────────

CLIENTS = {
    "Niedersachsen": NiedersachsenClient(),
    "Hamburg": HamburgClient(),
    "Bremen": BremenClient(),
    "Schleswig-Holstein": SchleswigHolsteinClient(),
    "Mecklenburg-Vorpommern": MecklenburgVorpommernClient(),
    "Nordrhein-Westfalen": NordrheinWestfalenClient(),
}


# ──────────────────────────────────────────────
# Hilfsfunktionen
# ──────────────────────────────────────────────

def _format_flurstueck_text(d: dict) -> str:
    """Erzeugt einen lesbaren String für ein Flurstück (für Pipedrive-Textfelder)."""
    parts = []
    if d.get("gemarkung"):
        parts.append(f"Gemarkung {d['gemarkung']}")
    if d.get("flur"):
        parts.append(f"Flur {d['flur']}")
    if d.get("flurstueck"):
        parts.append(f"Flurstück {d['flurstueck']}")
    return ", ".join(parts) or "unbekannt"


def _extract_address(field_value) -> str:
    """Extrahiert die Adresse aus einem Pipedrive-Adressfeld.

    Pipedrive liefert Adressfelder je nach Feldtyp als String oder Dict.

    Beispiel 1 – String (manuell eingetragen):
        "Musterstraße 1, 21680 Stade"
        → "Musterstraße 1, 21680 Stade"

    Beispiel 2 – vollständiges Dict (Google-Maps-formatiertes Adressfeld):
        {"value": "Musterstraße 1, 21680 Stade, Deutschland",
         "route": "Musterstraße", "street_number": "1",
         "postal_code": "21680", "locality": "Stade", "country": "Deutschland"}
        → "Musterstraße 1, 21680 Stade, Deutschland"

    Beispiel 3 – Dict ohne "value" (unvollständig ausgefüllt):
        {"route": "Musterstraße", "street_number": "1",
         "postal_code": "21680", "locality": "Stade"}
        → "Musterstraße 1, 21680 Stade"

    Beispiel 4 – leeres Dict oder None:
        {} oder None → ""
    """
    if isinstance(field_value, str):
        return field_value.strip()

    if not isinstance(field_value, dict):
        return ""

    # Bevorzuge den vorformatierten Gesamt-String
    if field_value.get("value"):
        return field_value["value"].strip()

    # Fallback: aus Einzelfeldern zusammenbauen
    route = field_value.get("route", "")
    street_number = field_value.get("street_number", "")
    postal_code = field_value.get("postal_code", "")
    locality = field_value.get("locality", "")

    street = f"{route} {street_number}".strip()
    city = f"{postal_code} {locality}".strip()
    parts = [p for p in [street, city] if p]
    return ", ".join(parts)


# ──────────────────────────────────────────────
# API Endpunkte
# ──────────────────────────────────────────────

@app.get("/")
def root(api_key: str = Depends(verify_api_key)):
    """Startseite / Health-Check."""
    return {
        "service": "Kataster-Lookup-Service",
        "version": "1.0.0",
        "status": "online",
        "unterstuetzte_bundeslaender": SUPPORTED_STATES,
        "nutzung": "GET /kataster?adresse=Musterstraße 1, 21680 Stade",
    }


@app.get("/kataster")
def kataster_lookup(
    adresse: str = Query(
        ...,
        description="Vollständige Adresse, z.B. 'Musterstraße 1, 21680 Stade'",
        min_length=5,
    ),
    gebaeude: bool = Query(
        False,
        description="Wenn true, wird zusätzlich die Gebäudegrundfläche abgefragt.",
    ),
    api_key: str = Depends(verify_api_key),
):
    """
    Ermittelt Katasterdaten anhand einer Adresse.

    Workflow:
    1. Geokodierung der Adresse über Nominatim → Koordinaten + Bundesland
    2. Räumliche Abfrage an den ALKIS-WFS des zuständigen Bundeslandes
    3. Rückgabe der Katasterdaten als JSON

    Returns:
        JSON mit Gemarkung, Flur, Flurstück, Fläche etc.
    """
    timestamp = datetime.now().isoformat()

    logger.info("Abfrage: %s", adresse)

    # Schritt 1: Geokodierung
    geo = geocode(adresse)
    if not geo:
        raise HTTPException(
            status_code=404,
            detail={
                "fehler": "Adresse nicht gefunden",
                "adresse": adresse,
                "hinweis": "Bitte Adresse mit Straße, Hausnummer, PLZ und Ort eingeben.",
            },
        )

    logger.info("Geocoder: %.6f, %.6f (%s, %s)", geo.lat, geo.lon, geo.bundesland, geo.ort)

    # Schritt 2: Bundesland prüfen
    if not is_supported(geo.bundesland):
        raise HTTPException(
            status_code=422,
            detail={
                "fehler": f"Bundesland '{geo.bundesland}' wird nicht unterstützt",
                "adresse": adresse,
                "koordinaten": {"lat": geo.lat, "lon": geo.lon},
                "unterstuetzte_bundeslaender": SUPPORTED_STATES,
            },
        )

    # Schritt 3: WFS-Abfrage
    client = CLIENTS.get(geo.bundesland)
    if not client:
        raise HTTPException(
            status_code=500,
            detail=f"Kein WFS-Client für {geo.bundesland} konfiguriert.",
        )

    logger.info("Router: verwende Client %s", client.bundesland_name)

    flurstuecke = client.query_flurstuecke(geo.lat, geo.lon, adresse=adresse)
    if not flurstuecke:
        raise HTTPException(
            status_code=404,
            detail={
                "fehler": "Kein Flurstück an dieser Position gefunden",
                "adresse": adresse,
                "koordinaten": {"lat": geo.lat, "lon": geo.lon},
                "bundesland": geo.bundesland,
                "hinweis": (
                    "Die Koordinaten liegen möglicherweise nicht auf einem "
                    "im ALKIS erfassten Flurstück, oder der WFS-Dienst ist "
                    "vorübergehend nicht erreichbar."
                ),
            },
        )

    # Schritt 4 (optional): Gebäudegrundfläche
    if gebaeude:
        logger.info("Gebaeude: suche Grundflaeche...")
        gf = client.query_gebaeude(geo.lat, geo.lon)
        if gf:
            flurstuecke[0].gebaeude_grundflaeche = gf
            logger.info("Gebaeude: Grundflaeche %.0f m²", gf)
        else:
            logger.info("Gebaeude: keine Grundflaeche gefunden")

    # Ergebnis zusammenstellen
    kataster_list = [f.to_dict() for f in flurstuecke]

    weitere_text = None
    if len(kataster_list) > 1:
        weitere_text = "; ".join(
            _format_flurstueck_text(f) for f in kataster_list[1:]
        )

    result = {
        "status": "ok",
        "abfrage": {
            "adresse": adresse,
            "aufgeloeste_adresse": geo.display_name,
            "koordinaten": {"lat": geo.lat, "lon": geo.lon},
            "ort": geo.ort,
            "plz": geo.plz,
        },
        "bundesland": geo.bundesland,
        "anzahl_flurstuecke": len(kataster_list),
        "weitere_flurstuecke_text": weitere_text,
        "kataster": kataster_list[0],  # Abwärtskompatibilität; bevorzuge kataster_ergebnisse[0]
        "kataster_ergebnisse": kataster_list,
        "zeitstempel": timestamp,
    }

    for f in flurstuecke:
        logger.info(
            "Ergebnis: %s / Flur %s / Flurstueck %s / %s",
            f.gemarkung, f.flur, f.flurstueck_display, f.flaeche_display,
        )

    return JSONResponse(content=result)


@app.post("/pipedrive/webhook")
async def pipedrive_webhook(
    request: Request,
    _auth: None = Depends(verify_webhook_auth),
):
    """
    Empfängt Pipedrive-Webhooks und schreibt Katasterdaten zurück in den Deal.

    Auth: HTTP Basic Auth (Pipedrive-kompatibel).
    Webhook-URL-Format: https://USER:PASSWORD@dein-service.railway.app/pipedrive/webhook

    Pipedrive sendet bei Deal-Ereignissen (created/updated) einen POST mit:
      payload["current"]["id"]                          → Deal-ID
      payload["current"][PIPEDRIVE_ADDRESS_FIELD_KEY]   → Adresse (String oder Dict)
      payload["previous"][PIPEDRIVE_ADDRESS_FIELD_KEY]  → Vorherige Adresse (Idempotenz)

    Konfiguration (Env-Variablen):
      PIPEDRIVE_WEBHOOK_USER         Basic-Auth-Benutzername
      PIPEDRIVE_WEBHOOK_PASSWORD     Basic-Auth-Passwort
      PIPEDRIVE_WEBHOOK_SECRET       Optional: Shared Secret für HMAC-Signatur-Verifikation
      PIPEDRIVE_ADDRESS_FIELD_KEY    Pipedrive-Feldschlüssel des Adressfelds
      PIPEDRIVE_GEMARKUNG_FIELD_KEY  Pipedrive-Feldschlüssel des Gemarkung-Felds (Idempotenz)
      PIPEDRIVE_API_TOKEN            Pipedrive API Token für Rückschreiben
      PIPEDRIVE_COMPANY_DOMAIN       z.B. "beierpartner"
      PIPEDRIVE_FIELD_MAP            JSON-Map: Kataster-Feldname → Pipedrive-Feldschlüssel

    Test-Curl (lokal ohne Credentials):
      curl -X POST -H "Content-Type: application/json" \\
        -d '{"current":{"id":123,"ADDR_KEY":"Musterstr. 1, 21680 Stade"},"previous":{}}' \\
        http://localhost:8000/pipedrive/webhook
    """
    from pipedrive_client import update_deal_fields

    # Body für HMAC-Verifikation lesen bevor JSON-Parsing
    body_bytes = await request.body()
    _verify_webhook_signature(request.headers, body_bytes)

    try:
        payload = json.loads(body_bytes)
    except (ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="Ungültiges JSON im Request-Body")

    address_field_key = os.environ.get("PIPEDRIVE_ADDRESS_FIELD_KEY", "")
    if not address_field_key:
        logger.error("PIPEDRIVE_ADDRESS_FIELD_KEY nicht konfiguriert")
        raise HTTPException(status_code=500, detail="PIPEDRIVE_ADDRESS_FIELD_KEY nicht konfiguriert")

    current = payload.get("current", {})
    previous = payload.get("previous", {})
    deal_id = current.get("id")

    adresse = _extract_address(current.get(address_field_key))

    if not deal_id or not adresse:
        logger.info("Webhook: deal_id oder Adresse fehlt — übersprungen")
        return {"status": "skipped", "reason": "deal_id oder Adresse fehlt"}

    # Idempotenz: Adresse unverändert?
    prev_adresse = _extract_address(previous.get(address_field_key)) if address_field_key in previous else None
    if prev_adresse is not None and prev_adresse == adresse:
        gemarkung_field = os.environ.get("PIPEDRIVE_GEMARKUNG_FIELD_KEY", "")
        if not gemarkung_field or current.get(gemarkung_field):
            logger.info(
                "Webhook: Deal %s — Adresse unverändert%s, übersprungen",
                deal_id,
                " und Gemarkung bereits vorhanden" if gemarkung_field and current.get(gemarkung_field) else "",
            )
            return {"status": "skipped", "reason": "Adresse unverändert"}

    logger.info("Webhook: Deal %s, Adresse '%s'", deal_id, adresse)

    geo = geocode(adresse)
    if not geo:
        logger.warning("Webhook: Adresse nicht geokodierbar: %s", adresse)
        return {"status": "skipped", "reason": "Adresse nicht gefunden"}

    if not is_supported(geo.bundesland):
        logger.info("Webhook: Bundesland '%s' nicht unterstützt", geo.bundesland)
        return {"status": "skipped", "reason": f"Bundesland '{geo.bundesland}' nicht unterstützt"}

    client = CLIENTS.get(geo.bundesland)
    if not client:
        return {"status": "error", "reason": f"Kein Client für {geo.bundesland}"}

    flurstuecke = client.query_flurstuecke(geo.lat, geo.lon, adresse=adresse)
    if not flurstuecke:
        logger.warning("Webhook: Kein Flurstück gefunden für %s", adresse)
        return {"status": "not_found", "reason": "Kein Flurstück an dieser Position"}

    kataster_data = flurstuecke[0].to_dict()
    kataster_data["bundesland"] = geo.bundesland

    success = update_deal_fields(int(deal_id), kataster_data)
    status = "ok" if success else "error"
    logger.info("Webhook: Deal %s → %s", deal_id, status)
    return {"status": status, "deal_id": deal_id}


@app.get("/health")
def health_check():
    """Einfacher Health-Check für Monitoring."""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


# ──────────────────────────────────────────────
# Server starten (direkte Ausführung)
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    logger.info("Kataster-Lookup-Service startet auf Port %d", port)
    logger.info("Unterstuetzte Bundeslaender: %s", ", ".join(SUPPORTED_STATES))
    logger.info("API-Dokumentation: http://localhost:%d/docs", port)

    uvicorn.run(app, host="0.0.0.0", port=port)
