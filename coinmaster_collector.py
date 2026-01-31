import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (CoinMasterCollector)"
}

SOURCES = {
    "BestCMStrategies": "https://bestcmstrategies.com/coin-master-free-spins-links/",
    "PocketTactics": "https://www.pockettactics.com/coin-master/free-spins",
    "Escapist": "https://www.escapistmagazine.com/coin-master-daily-free-spins-coin-links/"
}

def fetch_links(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = set()
    for a in soup.find_all("a", href=True):
        if "rewards.coinmaster.com" in a["href"]:
            links.add(a["href"].strip())

    return links

def build_html(links):
    rows = ""
    for i, link in enumerate(links, start=1):
        rows += f"""
        <tr>
            <td>{i}</td>
            <td><a href="{link['url']}" target="_blank">{link['url']}</a></td>
            <td>{link['source']}</td>
        </tr>
        """

    return f"""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Coin Master – Daily Reward Links</title>
<style>
body {{ font-family: Arial, sans-serif; background:#111; color:#eee; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ padding: 10px; border: 1px solid #444; }}
th {{ background: #222; }}
a {{ color: #4da6ff; word-break: break-all; }}
</style>
</head>
<body>
<h2>🎁 Coin Master – Daily Reward Links</h2>
<p>Aktualisiert: {datetime.utcnow().isoformat()} UTC</p>
<table>
<tr>
<th>#</th><th>Link</th><th>Quelle</th>
</tr>
{rows}
</table>
</body>
</html>
"""

def main():
    unique_links = {}
    results = []

    for source, url in SOURCES.items():
        print(f"[+] Sammle von {source}")
        try:
            links = fetch_links(url)
            for link in links:
                if link not in unique_links:
                    unique_links[link] = source
            time.sleep(2)
        except Exception as e:
            print(f"[!] Fehler bei {source}: {e}")

    for url, source in unique_links.items():
        results.append({
            "url": url,
            "source": source
        })

    results.sort(key=lambda x: x["source"])

    # JSON
    json_data = {
        "updated": datetime.utcnow().isoformat() + "Z",
        "count": len(results),
        "links": results
    }

    with open("spins_today.json", "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    # HTML
    html = build_html(results)
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ Fertig: {len(results)} eindeutige Links gespeichert")
    print("📄 index.html ist klickbar")

if __name__ == "__main__":
    main()
