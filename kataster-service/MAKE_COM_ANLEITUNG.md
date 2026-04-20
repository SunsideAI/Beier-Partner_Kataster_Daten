# Make.com Integration: Kataster-Lookup → Pipedrive

## Übersicht

Diese Anleitung beschreibt die Anbindung des Kataster-Lookup-Service an Pipedrive über Make.com.

**Workflow:**
Pipedrive (neuer Deal) → Make.com (Trigger) → HTTP-Request an Kataster-Service → Katasterdaten zurück → Pipedrive-Deal-Felder aktualisieren

---

## Voraussetzung: Pipedrive Custom Fields

Lege in Pipedrive folgende benutzerdefinierte Felder an (Deal-Ebene):

| Feldname | Typ | Beschreibung |
|---|---|---|
| Gemarkung | Text | z.B. "Schölisch" |
| Gemarkungsnummer | Text | z.B. "030510" |
| Flur | Text | z.B. "8" |
| Flurstück | Text | z.B. "54/17" |
| Flurstückskennzeichen | Text | z.B. "030510008000540017__" |
| Flurstücksfläche (m²) | Numerisch | z.B. 594 |
| Kataster-Lagebezeichnung | Text | z.B. "Schölischer Straße 101A" |
| Kataster-Nutzungsart | Text | z.B. "Wohnbaufläche" |
| Kataster-Gemeinde | Text | z.B. "Stade, Hansestadt" |
| Kataster-Quelle | Text | z.B. "LGLN Niedersachsen, Open Data" |

---

## Voraussetzung: Firewall / IP-Whitelist

Make.com sendet HTTP-Requests von festen IP-Adressen. Diese müssen auf eurem Windows Server in der Firewall freigegeben werden.

**Make.com IP-Adressen (EU-Region):**
Aktuelle Liste: https://www.make.com/en/help/troubleshooting/ip-addresses

Typischerweise sind das IP-Bereiche wie:
- 54.75.32.0/20 (EU-West)
- Weitere je nach Make.com-Datacenter

Bitte die aktuellen IPs auf der Make.com-Hilfeseite nachschlagen und in eurer Firewall für Port 8000 (oder euren gewählten Port) freigeben.

---

## Schritt 1: Make.com Szenario erstellen

1. In Make.com einloggen → **Create a new scenario**
2. Einen Trigger wählen (siehe Optionen unten)

### Trigger-Option A: Pipedrive-Trigger (automatisch)

- Modul: **Pipedrive → Watch Deals**
- Trigger: "New Deal" oder "Updated Deal"
- Filter: Nur Deals, bei denen das Adressfeld gefüllt ist

### Trigger-Option B: Manueller Webhook

- Modul: **Webhooks → Custom Webhook**
- Du bekommst eine URL, die du aus Pipedrive-Automationen oder manuell aufrufen kannst

---

## Schritt 2: HTTP-Request an Kataster-Service

Füge ein **HTTP → Make a request**-Modul hinzu:

| Einstellung | Wert |
|---|---|
| URL | `http://EUER-SERVER:8000/kataster` |
| Method | GET |
| Query String | `adresse` = (Adressfeld aus Pipedrive-Deal) |

### Query String konfigurieren

- **Key:** `adresse`
- **Value:** Mapped aus dem Pipedrive-Trigger, z.B. `{{1.address}}` oder das benutzerdefinierte Adressfeld eures Deals

### Erweiterte Einstellungen

| Einstellung | Wert |
|---|---|
| Parse response | Yes |
| Timeout | 30 Sekunden |

---

## Schritt 3: Pipedrive-Deal aktualisieren

Füge ein **Pipedrive → Update a Deal**-Modul hinzu:

| Einstellung | Wert |
|---|---|
| Deal ID | `{{1.id}}` (vom Trigger) |
| Gemarkung | `{{2.body.kataster.gemarkung}}` |
| Gemarkungsnummer | `{{2.body.kataster.gemarkungsnummer}}` |
| Flur | `{{2.body.kataster.flur}}` |
| Flurstück | `{{2.body.kataster.flurstueck}}` |
| Flurstückskennzeichen | `{{2.body.kataster.flurstueckskennzeichen}}` |
| Flurstücksfläche | `{{2.body.kataster.amtliche_flaeche_qm}}` |
| Lagebezeichnung | `{{2.body.kataster.lagebezeichnung}}` |
| Nutzungsart | `{{2.body.kataster.nutzungsart}}` |
| Gemeinde | `{{2.body.kataster.gemeinde}}` |
| Quelle | `{{2.body.kataster.quelle}}` |

**Hinweis:** Die Nummern (1, 2) beziehen sich auf die Modul-Reihenfolge in Make.com. Modul 1 = Trigger, Modul 2 = HTTP-Request.

---

## Schritt 4: Fehlerbehandlung

Füge einen **Error Handler** (Router → Error Handler) hinzu:

### Mögliche Fehler

| HTTP-Status | Bedeutung | Aktion |
|---|---|---|
| 200 | Erfolgreich | → Pipedrive aktualisieren |
| 404 | Adresse oder Flurstück nicht gefunden | → Notiz am Deal: "Katasterabfrage: Adresse nicht gefunden" |
| 422 | Bundesland nicht unterstützt | → Notiz am Deal: "Bundesland nicht abgedeckt" |
| 500 / Timeout | Server nicht erreichbar | → Wiederholung oder Benachrichtigung |

### Error Handler konfigurieren

1. Klick auf das HTTP-Modul → **Add error handler**
2. **Resume** → Szenario fortsetzen, auch wenn die Abfrage fehlschlägt
3. Optional: Bei Fehler eine Pipedrive-Notiz erstellen mit der Fehlermeldung

---

## Schritt 5: Testen

1. In Make.com: **Run once** klicken
2. In Pipedrive: Einen Test-Deal mit vollständiger Adresse anlegen
3. Prüfen, ob die Katasterfelder im Deal befüllt werden

### Test-Adressen

| Adresse | Erwartetes Ergebnis |
|---|---|
| Schölischer Str. 101A, 21682 Stade | Gemarkung Schölisch, Flurstück 54/17 |
| Josthöhe 116, 22339 Hamburg | Gemarkung Hummelsbüttel, Flurstück 1854 |
| Damm 8, 25813 Husum | Gemarkung Husum, Flurstück 10/7 |
| Neustadtswall 30, 28199 Bremen | Gemarkungsnr. 4035, Flurstück 51/29 |
| Philipp-Müller-Str. 12, 23966 Wismar | Gemarkung Wismar, Flurstück 2636/58 |

---

## Komplettes Szenario (Zusammenfassung)

```
[Pipedrive: Watch Deals]
        ↓
[HTTP: GET http://SERVER:8000/kataster?adresse={{deal.address}}]
        ↓
    ┌── Status 200 ──→ [Pipedrive: Update Deal mit Katasterdaten]
    │
    └── Fehler ──→ [Pipedrive: Create Note "Katasterabfrage fehlgeschlagen"]
```

---

## Alternative: n8n statt Make.com

Falls ihr n8n nutzt, ist der Ablauf identisch:

1. **Trigger:** Pipedrive Trigger Node (neue Deals)
2. **HTTP Request Node:** GET an `http://SERVER:8000/kataster?adresse={{...}}`
3. **Pipedrive Node:** Deal aktualisieren mit den Antwortdaten
4. **Error Handling:** If-Node prüft `status == "ok"`

---

## Hinweise

- **Rate-Limiting:** Nominatim erlaubt max. 1 Request/Sekunde. Bei Massenimports ggf. Verzögerung einbauen (Make.com: Sleep-Modul mit 1500ms).
- **Adressformat:** Je vollständiger die Adresse (Straße, Hausnummer, PLZ, Ort), desto besser die Ergebnisse.
- **Hausnummernzusätze:** Der Service erkennt Zusätze wie "101A" oder "12b" und matcht sie gegen die ALKIS-Lagebezeichnung.
