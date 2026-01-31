import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import time
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (CoinMasterCollector)"
}

SOURCES = {
    "BestCMStrategies": "https://bestcmstrategies.com/coin-master-free-spins-links/",
    "PocketTactics": "https://www.pockettactics.com/coin-master/free-spins",
    "Escapist": "https://www.escapistmagazine.com/coin-master-daily-free-spins-coin-links/"
}

JSON_FILE = "spins_today.json"
MAX_AGE_HOURS = 24

def fetch_links(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = set()
    for a in soup.find_all("a", href=True):
        if "rewards.coinmaster.com" in a["href"]:
            links.add(a["href"].strip())

    return links

def load_existing():
    if not os.path.exists(JSON_FILE):
        return {}

    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    existing = {}
    for item in data.get("links", []):
        existing[item["url"]] = datetime.fromisoformat(item["first_seen"])
    return existing

def build_html(links):
    rows = ""
    for i, link in enumerate(links, start=1):
        rows += f"""
        <tr>
            <td>{i}</td>
            <td><a href="{link['url']}" target="_blank">{link['url']}</a></td>
            <td>{link['source']}</td>
            <td>{link['first_seen']}</td>
        </tr>
        """

    return f"""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Coin Master – Rewards (letzte 24h)</title>
<style>
body {{ font-family: Arial, sans-serif; background:#111; color:#eee; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ padding: 10px; border: 1px solid #444; }}
th {{ background: #222; }}
a {{ color: #4da6ff; word-break: break-all; }}
</style>
</head>
<body>
<h2>🎁 Coin Master – Reward Links (letzte 24h)</h2>
<p>Letztes Update: {datetime.utcnow().isoformat()} UTC</p>
<table>
<tr>
<th>#</th><th>Link</th><th>Quelle</th><th>First Seen (UTC)</th>
</tr>
{rows}
</table>
</body>
</html>
"""

def main():
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=MAX_AGE_HOURS)

    existing_links = load_existing()
    collected = {}

    for source, url in SOURCES.items():
        print(f"[+] Sammle von {source}")
        try:
            links = fetch_links(url)
            for link in links:
                if link in existing_links:
                    collected[link] = existing_links[link]
                else:
                    collected[link] = now
            time.sleep(2)
        except Exception as e:
            print(f"[!] Fehler bei {source}: {e}")

    # Filter: nur letzte 24h
    final_links = []
    for url, first_seen in collected.items():
        if first_seen >= cutoff:
            final_links.append({
                "url": url,
                "source": next((s for s,u in SOURCES.items() if s), "unknown"),
                "first_seen": first_seen.isoformat()
            })

    final_links.sort(key=lambda x: x["first_seen"], reverse=True)

    data = {
        "updated": now.isoformat() + "Z",
        "count": len(final_links),
        "links": final_links
    }

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(build_html(final_links))

    print(f"\n✅ Update fertig – {len(final_links)} Links (≤24h)")

if __name__ == "__main__":
    main()
