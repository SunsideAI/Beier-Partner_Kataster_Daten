# Kataster-Lookup-Service v2.0

REST-API zur Abfrage von Katasterdaten (Gemarkung, Flur, Flurstück, Fläche) anhand einer Adresse.
Nutzt die Open-Data-WFS-Dienste der norddeutschen Landesvermessungsämter.

## Unterstützte Bundesländer

| Bundesland | Technik | Lizenz | Gemarkungsname | Lagebezeichnung |
|---|---|---|:-:|:-:|
| Niedersachsen | WFS vereinfacht (LGLN) | Open Data | Ja | Ja |
| Hamburg | OGC API Features (LGV) | DL-DE-BY 2.0 | Ja | Ja |
| Bremen | Volles ALKIS-Schema (LGLN) | CC BY 4.0 | nur Nr. | - |
| Schleswig-Holstein | INSPIRE WFS + CadastralZoning | CC BY 4.0 | Ja | - |
| Mecklenburg-Vorpommern | INSPIRE WFS + CadastralZoning | CC BY 4.0 * | Ja | - |

* Gewerbliche Nutzung der MV-Daten in Gutachten ggf. klaerungsbeduerftig.

## Installation (Windows Server)

### 1. Python installieren
- Python 3.11+ von https://www.python.org/downloads/
- Bei Installation "Add Python to PATH" anhaken

### 2. Abhaengigkeiten installieren
```cmd
cd C:\KatasterService
python -m pip install --upgrade pip
python -m pip install shapely --only-binary=shapely
python -m pip install fastapi uvicorn requests lxml pyproj
```

### 3. Service starten
```cmd
python main.py
```
API-Docs: http://localhost:8000/docs

### 4. Als Windows-Dienst (NSSM)
```cmd
nssm install KatasterService "C:\Python3xx\python.exe" "C:\KatasterService\main.py" 8000
nssm set KatasterService AppDirectory "C:\KatasterService"
nssm start KatasterService
```

## API-Endpunkt
```
GET /kataster?adresse=Musterstrasse 1, 21680 Stade
GET /kataster?adresse=Musterstrasse 1, 21680 Stade&gebaeude=true
```

## Lokales Testen (Docker)

```bash
# Service starten
docker compose up --build

# Health-Check (kein API-Key erforderlich)
curl http://localhost:8000/health

# Kataster-Abfrage — Niedersachsen
curl -H "X-API-Key: localdev-secret" \
  "http://localhost:8000/kataster?adresse=Schölischer+Str.+101A,+21682+Stade"

# Kataster-Abfrage — Hamburg
curl -H "X-API-Key: localdev-secret" \
  "http://localhost:8000/kataster?adresse=Mönckebergstraße+7,+20095+Hamburg"

# Kataster-Abfrage mit Gebäudegrundfläche
curl -H "X-API-Key: localdev-secret" \
  "http://localhost:8000/kataster?adresse=Schölischer+Str.+101A,+21682+Stade&gebaeude=true"

# Fehlerfall: Adresse nicht gefunden (404)
curl -H "X-API-Key: localdev-secret" \
  "http://localhost:8000/kataster?adresse=Xxxxxxstraße+99,+00000+Nirgendwo"

# Fehlerfall: nicht unterstütztes Bundesland (422)
curl -H "X-API-Key: localdev-secret" \
  "http://localhost:8000/kataster?adresse=Marienplatz+1,+80331+München"

# Fehlerfall: fehlender API-Key (401)
curl "http://localhost:8000/kataster?adresse=Stade"
```

## Quellenvermerke
- Niedersachsen: LGLN (Open Data)
- Hamburg: LGV (Datenlizenz Deutschland BY 2.0)
- Bremen: Landesamt GeoInformation Bremen (CC BY 4.0)
- Schleswig-Holstein: GeoBasis-DE/LVermGeo SH/CC BY 4.0
- Mecklenburg-Vorpommern: GeoBasis-DE/M-V (CC BY 4.0)
