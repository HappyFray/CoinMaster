#!/usr/bin/env python3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests, json, os

DATA_FILE = "links.json"
SOURCES = [
    {"name": "TechGameWorld", "url": "https://techgameworld.com/coin-master-free-spins/"},
    {"name": "Giveaway48",    "url": "https://giveaway48.com/coin-master/"}
]

# 1. Bestehende Daten laden (oder neu anlegen)
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
else:
    data = {"links": []}

now = datetime.utcnow()

# 2. Neue Links von definierten Quellen sammeln
for src in SOURCES:
    try:
        res = requests.get(src["url"], timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"Fehler beim Laden von {src['name']}: {e}")
        continue
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("http"):
            continue
        # Bekannte interne oder App-Store-Links überspringen
        if any(ex in href for ex in ["coinmastergame.com", "facebook.com", "twitter.com",
                                      "instagram.com", "play.google.com", "apps.apple.com"]):
            continue
        # Neuen Link hinzufügen
        if not any(entry["url"] == href for entry in data["links"]):
            data["links"].append({
                "url": href,
                "source": src["name"],
                "first_seen": now.isoformat()
            })

# 3. Links prüfen und aktualisieren
valid_links = []
removed_count = expired_count = 0
for entry in data["links"]:
    first_seen = datetime.fromisoformat(entry["first_seen"])
    age = now - first_seen
    # Link abgelaufen nach 72 Stunden
    if age > timedelta(hours=72):
        expired_count += 1
        continue
    # HTTP-Status prüfen (200=OK)
    try:
        resp = requests.head(entry["url"], allow_redirects=True, timeout=5)
        status = resp.status_code
        if status != 200:
            # Bei fehlgeschlagenem HEAD mit GET versuchen
            resp = requests.get(entry["url"], allow_redirects=True, timeout=5)
            status = resp.status_code
        if status == 200:
            entry["status_code"] = 200
            valid_links.append(entry)
        else:
            removed_count += 1
    except Exception:
        removed_count += 1

# 4. Statistiken berechnen
total_valid = len(valid_links)
by_source = {}
for e in valid_links:
    by_source[e["source"]] = by_source.get(e["source"], 0) + 1

# 5. JSON-Ausgabe erstellen
output = {
    "generated": now.isoformat(),
    "total_links": total_valid,
    "by_source": by_source,
    "removed": removed_count,
    "expired": expired_count,
    "links": valid_links
}
with open(DATA_FILE, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

# 6. HTML-Ausgabe erstellen (mit Bootstrap)
html_rows = ""
for e in valid_links:
    first_seen = datetime.fromisoformat(e["first_seen"])
    age_h = (now - first_seen).total_seconds() / 3600
    # Verbleibende Stunden
    hours_left = int((timedelta(hours=72) - (now - first_seen)).total_seconds() // 3600)
    remaining = f"noch {hours_left}h" if hours_left >= 1 else "weniger 1h"
    # Ampelfarbe
    if age_h < 24:
        amp = "🟢"; title = "<24h"
    elif age_h < 48:
        amp = "🟡"; title = "24–48h"
    else:
        amp = "🔴"; title = ">48h"
    html_rows += f"""
      <tr>
        <td>{e['source']}</td>
        <td><a href="{e['url']}" target="_blank">Öffnen</a></td>
        <td>{first_seen.strftime('%Y-%m-%d %H:%M')}</td>
        <td>{remaining}</td>
        <td><span title="{title}">{amp}</span></td>
      </tr>"""

html_content = f"""<!DOCTYPE html>
<html lang="de"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Coin Master Reward Links</title>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
</head><body>
  <div class="container">
    <h1>Coin Master Reward Links</h1>
    <p>Gültige Links: {total_valid} (entfernt: {removed_count}, abgelaufen: {expired_count})</p>
    <p>Pro Quelle: {" | ".join(f"{k}: {v}" for k,v in by_source.items())}</p>
    <table class="table table-striped table-hover">
      <thead><tr><th>Quelle</th><th>Link</th><th>Erstellt am</th><th>Verbleibend</th><th>Ampel</th></tr></thead>
      <tbody>{html_rows}
      </tbody>
    </table>
  </div>
</body></html>"""

with open("links.html", "w", encoding="utf-8") as f:
    f.write(html_content)
