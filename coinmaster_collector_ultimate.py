#!/usr/bin/env python3
"""
coinmaster_collector_ultimate.py

FINAL - ALL FEATURES IMPLEMENTED & PACKED INTO CANVAS

Dieses Script ist die vollendete Version. Es beinhaltet:
 - Scraper + Heuristik
 - Live-Checker (follow redirects)
 - Persistente SQLite (links, domains, runs)
 - Nur Accept: finale Domain == static.moonactive.net
 - CLI: --web, --dry
 - Web-UI (Flask) mit:
    * Live health endpoint (/health)
    * Auto-refresh
    * Copy-all Button
    * CSV / JSON Export
    * Manual invalidate
    * Cleanup trigger
 - Auto-Cleanup (CLI-invoked) & Dry-run mode

Start: python3 coinmaster_collector_ultimate.py [--web] [--dry]

"""

# =====================
# IMPORTS
# =====================
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import argparse, logging, os, re, requests, sqlite3, threading, time, io, csv, json
from flask import Flask, render_template_string, redirect, url_for, request, Response, jsonify

# =====================
# CONFIG
# =====================
SOURCES = [
    {"name": "TechGameWorld", "url": "https://techgameworld.com/coin-master-free-spins/"},
    {"name": "Giveaway48", "url": "https://giveaway48.com/coin-master/"}
]

DB_FILE = "coinmaster.db"
HTML_FILE = "coinmaster.html"
LOG_FILE = "coinmaster.log"

MAX_WORKERS = 8
TIMEOUT = 8
MAX_AGE_HOURS = 72
SCORE_THRESHOLD = 4
ALLOWED_FINAL_DOMAIN = "static.moonactive.net"

TRACKING_PARAMS = {"utm_source","utm_medium","utm_campaign","fbclid","gclid","ref"}
BLACKLIST_DOMAINS = {"facebook.com","twitter.com","instagram.com","youtube.com","coinmastergame.com","apps.apple.com","play.google.com"}
FOOTER_KEYWORDS = {"privacy","imprint","terms","contact","about","cookie","site-notice"}
REWARD_PATTERNS = [r"free spins", r"free coins", r"coin master", r"claim", r"reward", r"spin"]

# =====================
# LOGGING
# =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE,encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("coinmaster")

# =====================
# HELPERS
# =====================

def normalize(url:str)->str:
    p=urlparse(url)
    qs=[(k,v) for k,v in parse_qsl(p.query) if k not in TRACKING_PARAMS]
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    path = p.path or "/"
    return urlunparse((scheme, netloc, path, "", urlencode(qs), ""))


def domain_of(url:str)->str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def is_reward_text(blob:str)->bool:
    b = (blob or '').lower()
    return any(re.search(p, b) for p in REWARD_PATTERNS)

# =====================
# STORAGE
# =====================
class DB:
    def __init__(self,path=DB_FILE):
        self.path = path
        self.conn = sqlite3.connect(self.path,check_same_thread=False)
        self.lock = threading.Lock()
        self._init()

    def _init(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS links(
            url TEXT PRIMARY KEY,
            source TEXT,
            domain TEXT,
            first_seen TEXT,
            last_checked TEXT,
            final_url TEXT,
            final_domain TEXT,
            valid INTEGER,
            score INTEGER,
            title TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS domains(
            domain TEXT PRIMARY KEY,
            trust INTEGER
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS runs(
            ts TEXT,
            checked INTEGER,
            valid INTEGER,
            duration REAL
        )''')
        self.conn.commit()

    def upsert_link(self,row:dict):
        with self.lock, self.conn:
            self.conn.execute('''INSERT INTO links(url,source,domain,first_seen,last_checked,final_url,final_domain,valid,score,title)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(url) DO UPDATE SET
                    last_checked=excluded.last_checked,
                    final_url=excluded.final_url,
                    final_domain=excluded.final_domain,
                    valid=excluded.valid,
                    score=excluded.score,
                    title=excluded.title
            ''', (
                row['url'], row['source'], row['domain'], row['first_seen'], row['last_checked'],
                row.get('final_url'), row.get('final_domain'), 1 if row['valid'] else 0, row['score'], row.get('title')
            ))

    def update_domain_trust(self, dom, delta):
        with self.lock, self.conn:
            cur = self.conn.cursor()
            cur.execute('SELECT trust FROM domains WHERE domain=?', (dom,))
            r = cur.fetchone()
            trust = (r[0] if r else 0) + delta
            cur.execute('INSERT OR REPLACE INTO domains(domain,trust) VALUES(?,?)', (dom, trust))

    def valid_links(self):
        cur = self.conn.cursor()
        return cur.execute('SELECT url,source,title,final_url FROM links WHERE valid=1').fetchall()

    def cleanup(self, dry=False):
        cutoff = (datetime.utcnow()-timedelta(hours=MAX_AGE_HOURS)).isoformat()
        if dry:
            cur = self.conn.cursor()
            return cur.execute('SELECT count(*) FROM links WHERE first_seen<? OR valid=0', (cutoff,)).fetchone()[0]
        with self.lock, self.conn:
            self.conn.execute('DELETE FROM links WHERE first_seen<? OR valid=0', (cutoff,))
            self.conn.commit()

    def last_run(self):
        cur = self.conn.cursor()
        r = cur.execute('SELECT ts,checked,valid,duration FROM runs ORDER BY ts DESC LIMIT 1').fetchone()
        return r

    def insert_run(self, checked, valid, duration):
        with self.lock, self.conn:
            self.conn.execute('INSERT INTO runs(ts,checked,valid,duration) VALUES(?,?,?,?)', (datetime.utcnow().isoformat(), checked, valid, duration))

# =====================
# COLLECTOR
# =====================
class Collector:
    def __init__(self, db:DB, workers:int=MAX_WORKERS, dry:bool=False):
        self.db = db
        self.s = requests.Session()
        self.s.headers.update({"User-Agent":"Mozilla/5.0"})
        self.workers = workers
        self.dry = dry

    def scrape(self):
        found = []
        for s in SOURCES:
            logger.info(f"Scraping {s['name']}")
            try:
                resp = self.s.get(s['url'], timeout=TIMEOUT)
                resp.raise_for_status()
                html = resp.text
            except Exception as e:
                logger.warning(f"Source {s['name']} failed: {e}")
                continue
            soup = BeautifulSoup(html, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if not href.startswith('http'): continue
                href = normalize(href)
                d = domain_of(href)
                if d in BLACKLIST_DOMAINS: continue
                if any(k in href.lower() for k in FOOTER_KEYWORDS): continue
                text = a.get_text(strip=True) or ''
                # candidate only if heuristic indicates reward-ish
                if not is_reward_text(href + ' ' + text): continue
                found.append((href, s['name'], text))
        logger.info(f"Scraped candidates: {len(found)}")
        return found

    def check_one(self, item):
        url, src, anchor_text = item
        try:
            r = self.s.get(url, timeout=TIMEOUT, allow_redirects=True)
            status = r.status_code
            final = r.url
            final_dom = domain_of(final)
            ct = r.headers.get('Content-Type', '')
            title = ''
            if 'text' in ct and r.text:
                try:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    title = soup.title.string.strip() if soup.title and soup.title.string else ''
                except Exception:
                    title = ''
            combined = ' '.join([final or '', title or '', anchor_text or ''])
            score = 5 if is_reward_text(combined) else 0
            allowed = (final_dom == ALLOWED_FINAL_DOMAIN)
            valid = (status == 200 and score >= SCORE_THRESHOLD and allowed)
            if not allowed:
                logger.debug(f"Discard {url} final domain {final_dom} != {ALLOWED_FINAL_DOMAIN}")
        except Exception as e:
            logger.debug(f"Error fetching {url}: {e}")
            status = None; final = url; final_dom = domain_of(final); title = ''; score = 0; valid = False

        row = {
            'url': url,
            'source': src,
            'domain': domain_of(url),
            'first_seen': datetime.utcnow().isoformat(),
            'last_checked': datetime.utcnow().isoformat(),
            'final_url': final,
            'final_domain': final_dom,
            'valid': 1 if valid else 0,
            'score': score,
            'title': title
        }

        if not self.dry and valid:
            self.db.upsert_link(row)
            self.db.update_domain_trust(row['domain'], 1)
            logger.info(f"Stored valid allowed link: {url} (final {final_dom})")
        else:
            if not self.dry:
                self.db.update_domain_trust(row['domain'], -1)
            logger.debug(f"Not stored (valid={valid},final={final_dom}): {url}")

        return valid

    def run(self):
        start = time.time()
        items = self.scrape()
        valid = 0
        total = len(items)
        if total == 0:
            self.db.insert_run(0,0,0.0)
            return 0,0,0.0
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            for ok in ex.map(self.check_one, items):
                if ok: valid += 1
        dur = time.time() - start
        self.db.insert_run(total, valid, dur)
        logger.info(f"Run complete — {valid}/{total} accepted (final domain == {ALLOWED_FINAL_DOMAIN}) in {dur:.1f}s")
        return valid, total, dur

# =====================
# WEB UI
# =====================
app = Flask(__name__)

MAIN_TPL = '''<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Coin Master - Allowed Links</title>
<style>body{background:#111;color:#eee;font-family:Arial;padding:18px}a{color:#6cf}table{width:100%;border-collapse:collapse}th,td{border:1px solid #333;padding:6px}</style>
<script>
function refresh(){fetch('/health').then(r=>r.json()).then(d=>{document.getElementById('status').innerText='Last run: '+(d.last_run||'n/a')+' — valid: '+(d.valid||0)+' — checked: '+(d.checked||0)}).catch(()=>{});}
setInterval(refresh,30000);
function copyAll(){let out='';document.querySelectorAll('a.link').forEach(a=>out+=a.href+"\n");navigator.clipboard.writeText(out)}
</script>
</head><body>
<h1>Gültige Reward Links (final domain: {{allowed}})</h1>
<div id="status">Loading status...</div>
<button onclick="copyAll()">Copy all links</button>
<button onclick="location='/export.csv'">Export CSV</button>
<button onclick="location='/export.json'">Export JSON</button>
<button onclick="location='/cleanup'">Run Cleanup (remove old/invalid)</button>
<table><thead><tr><th>Quelle</th><th>Titel / URL</th><th>Final URL</th><th>Aktion</th></tr></thead><tbody>
{% for url,source,title,final in rows %}
<tr>
<td>{{source}}</td>
<td><a class="link" href="{{url}}" target="_blank">{{title or url}}</a></td>
<td><a href="{{final}}" target="_blank">{{final}}</a></td>
<td><a href="/invalidate?u={{url}}">Invalidate</a></td>
</tr>
{% endfor %}
</tbody></table>
</body></html>'''

@app.route('/')
def index():
    rows = app.db.valid_links()
    return render_template_string(MAIN_TPL, rows=rows, allowed=ALLOWED_FINAL_DOMAIN)

@app.route('/invalidate')
def invalidate():
    u = request.args.get('u')
    if u:
        with app.db.conn:
            app.db.conn.execute('UPDATE links SET valid=0 WHERE url=?',(u,))
    return redirect(url_for('index'))

@app.route('/export.csv')
def export_csv():
    rows = app.db.valid_links()
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['url','source','title','final_url'])
    for url,source,title,final in rows:
        cw.writerow([url,source,title,final])
    return Response(si.getvalue(), mimetype='text/csv', headers={"Content-Disposition":"attachment; filename=coinmaster_links.csv"})

@app.route('/export.json')
def export_json():
    rows = app.db.valid_links()
    out = [{'url':u,'source':s,'title':t,'final_url':f} for u,s,t,f in rows]
    return jsonify(out)

@app.route('/health')
def health():
    last = app.db.last_run()
    counts = app.db.conn.execute('SELECT COUNT(*) FROM links WHERE valid=1').fetchone()[0]
    return jsonify({
        'last_run': last[0] if last else None,
        'checked': last[1] if last else 0,
        'valid': last[2] if last else 0,
        'duration': last[3] if last else 0.0,
        'total_valid_links': counts
    })

@app.route('/cleanup')
def cleanup_route():
    removed = app.db.cleanup(dry=False)
    return redirect(url_for('index'))

# =====================
# CLI
# =====================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--web', action='store_true', help='Start local Web UI after run')
    p.add_argument('--dry', action='store_true', help='Dry-run: do not persist changes')
    args = p.parse_args()

    db = DB()
    c = Collector(db, workers=MAX_WORKERS, dry=args.dry)
    valid, total, dur = c.run()
    removed = db.cleanup(dry=args.dry)
    if args.dry:
        logger.info(f"[DRY] would remove {removed} entries (age>={MAX_AGE_HOURS}h or invalid)")
    else:
        logger.info(f"Removed {removed} expired/invalid entries")

    if args.web:
        app.db = db
        logger.info('Starting local Web UI on http://127.0.0.1:5000')
        app.run(host='127.0.0.1', port=5000)

if __name__=='__main__':
    main()