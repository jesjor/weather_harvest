"""
╔══════════════════════════════════════════════════════════════════════╗
║  POLYMARKET WEATHER MARKET HARVESTER                                 ║
║  Indsamler data til backtesting af vejr-trading strategier           ║
╠══════════════════════════════════════════════════════════════════════╣
║  Datakilder:                                                         ║
║  • Polymarket Gamma API: aktive vejr-markets + metadata              ║
║  • Polymarket CLOB API: YES/NO orderbook for hvert temperature-band  ║
║  • NOAA/NWS API: timeprognoser + observationer (US byer)             ║
║  • Open-Meteo API: timeprognoser + observationer (Toronto, London)   ║
║                                                                      ║
║  Byer: Dallas, NYC, Chicago, Seattle, Atlanta, Miami, Toronto, London║
║  Interval: 5 minutter                                                ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import time, datetime, sqlite3, json, csv, os, sys, logging, argparse
import threading, http.server, zipfile, io
from pathlib import Path

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("FEJL: pip install requests")
    sys.exit(1)

# ─── KONFIGURATION ────────────────────────────────────────────────────

_DATA_DIR    = Path(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "data"))
DB_PATH      = _DATA_DIR / "weather_harvest.db"
LOG_PATH     = _DATA_DIR / "weather_harvest.log"
INTERVAL_SEC = 300  # 5 minutter

WEATHER_TAG_ID = 100381  # Polymarket weather tag

CITIES = {
    "NYC": {
        "lat": 40.7128, "lon": -74.0060, "source": "noaa",
        "noaa_station": "KNYC", "polymarket_name": "new york",
        "timezone": "America/New_York",
    },
    "Chicago": {
        "lat": 41.8781, "lon": -87.6298, "source": "noaa",
        "noaa_station": "KMDW", "polymarket_name": "chicago",
        "timezone": "America/Chicago",
    },
    "Dallas": {
        "lat": 32.7767, "lon": -96.7970, "source": "noaa",
        "noaa_station": "KDFW", "polymarket_name": "dallas",
        "timezone": "America/Chicago",
    },
    "Seattle": {
        "lat": 47.6062, "lon": -122.3321, "source": "noaa",
        "noaa_station": "KSEA", "polymarket_name": "seattle",
        "timezone": "America/Los_Angeles",
    },
    "Atlanta": {
        "lat": 33.7490, "lon": -84.3880, "source": "noaa",
        "noaa_station": "KATL", "polymarket_name": "atlanta",
        "timezone": "America/New_York",
    },
    "Miami": {
        "lat": 25.7617, "lon": -80.1918, "source": "noaa",
        "noaa_station": "KMIA", "polymarket_name": "miami",
        "timezone": "America/New_York",
    },
    "Toronto": {
        "lat": 43.6532, "lon": -79.3832, "source": "open_meteo",
        "polymarket_name": "toronto", "timezone": "America/Toronto",
    },
    "London": {
        "lat": 51.5074, "lon": -0.1278, "source": "open_meteo",
        "polymarket_name": "london", "wu_station": "EGLC",
        "timezone": "Europe/London",
    },
}

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
NWS_BASE   = "https://api.weather.gov"
OM_BASE    = "https://api.open-meteo.com/v1"
TIMEOUT    = 15

# ─── LOGGING ──────────────────────────────────────────────────────────

_DATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ─── HTTP SESSION ─────────────────────────────────────────────────────

def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "WeatherHarvester/1.0 (polymarket-backtesting)",
        "Accept": "application/json",
    })
    return s

SESSION = make_session()

# ─── HELPERS ──────────────────────────────────────────────────────────

def c_to_f(c): return round(c * 9/5 + 32, 2) if c is not None else None
def f_to_c(f): return round((f - 32) * 5/9, 2) if f is not None else None
def now_ts(): return int(time.time())
def now_dt(ts=None):
    return datetime.datetime.fromtimestamp(ts or now_ts(), datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ─── DATABASE ─────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS poly_weather_markets (
        ts INTEGER, dt TEXT, city TEXT,
        market_slug TEXT, condition_id TEXT PRIMARY KEY,
        question TEXT,
        temp_low REAL, temp_high REAL,   -- Fahrenheit
        temp_low_c REAL, temp_high_c REAL, -- Celsius
        resolution_date TEXT, end_date TEXT,
        yes_token_id TEXT, no_token_id TEXT,
        active INTEGER, volume REAL, liquidity REAL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS poly_weather_books (
        ts INTEGER, dt TEXT, city TEXT, condition_id TEXT,
        yes_bid REAL, yes_ask REAL, yes_mid REAL,
        yes_bid_size REAL, yes_ask_size REAL,
        no_bid REAL, no_ask REAL, no_mid REAL,
        no_bid_size REAL, no_ask_size REAL,
        sum_bid REAL,         -- yes_bid + no_bid  (< 1.0 = potentiel ARB)
        spread_yes REAL,      -- yes_ask - yes_bid
        spread_no REAL,       -- no_ask - no_bid
        implied_prob REAL,    -- yes_mid (markedets sandsynlighed for YES)
        PRIMARY KEY (ts, condition_id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS noaa_forecast (
        ts INTEGER, dt TEXT, city TEXT,
        forecast_hour TEXT,          -- ISO8601 tidspunkt prognosen gaelder for
        temp_f REAL, temp_c REAL,
        dew_point_f REAL, humidity REAL,
        wind_speed REAL,             -- mph
        wind_direction TEXT,
        precip_prob REAL,            -- % sandsynlighed for nedbor
        short_forecast TEXT,
        PRIMARY KEY (ts, city, forecast_hour)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS noaa_observations (
        ts INTEGER, dt TEXT, city TEXT,
        obs_time TEXT,
        temp_f REAL, temp_c REAL, dew_point_f REAL,
        humidity REAL,
        wind_speed REAL,             -- mph
        wind_direction REAL,         -- grader
        wind_gust REAL,
        pressure_mb REAL,
        visibility_m REAL,
        precip_1h REAL,              -- mm nedbor seneste time
        weather_desc TEXT,
        PRIMARY KEY (ts, city)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS noaa_griddata (
        ts INTEGER, dt TEXT, city TEXT, valid_time TEXT,
        temp_c REAL, dew_point_c REAL,
        max_temp_c REAL, min_temp_c REAL,
        precip_mm REAL, snow_mm REAL,
        sky_cover REAL,              -- %
        wind_speed_kmh REAL, wind_gust_kmh REAL, wind_dir REAL,
        PRIMARY KEY (ts, city, valid_time)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS openmeteo_forecast (
        ts INTEGER, dt TEXT, city TEXT, forecast_hour TEXT,
        temp_c REAL, temp_f REAL,
        apparent_temp_c REAL, dew_point_c REAL,
        humidity REAL, precip_prob REAL, precipitation REAL,
        wind_speed REAL,             -- km/h
        wind_direction REAL, wind_gusts REAL,
        surface_pressure REAL,       -- hPa
        cloud_cover REAL,            -- %
        visibility REAL,             -- meter
        weather_code INTEGER,        -- WMO weather code
        PRIMARY KEY (ts, city, forecast_hour)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS openmeteo_current (
        ts INTEGER, dt TEXT, city TEXT, obs_time TEXT,
        temp_c REAL, temp_f REAL, apparent_temp_c REAL,
        humidity REAL, precip REAL,
        wind_speed REAL, wind_direction REAL, wind_gusts REAL,
        surface_pressure REAL, cloud_cover REAL, weather_code INTEGER,
        PRIMARY KEY (ts, city)
    )""")

    # Udvidet outcomes tabel — inkl. faktisk temperatur og forecast-præcision
    c.execute("""CREATE TABLE IF NOT EXISTS poly_weather_outcomes (
        condition_id TEXT PRIMARY KEY,
        city TEXT, resolution_date TEXT,
        temp_low REAL, temp_high REAL,    -- Markedets band (F)
        resolved_yes INTEGER,             -- 1=YES vandt, 0=NO vandt, NULL=uafklaret
        resolution_ts INTEGER,
        -- Faktisk temperatur ved resolution (fra NOAA/Open-Meteo obs)
        actual_temp_f REAL,
        actual_temp_c REAL,
        actual_source TEXT,               -- 'noaa_obs', 'openmeteo_obs'
        -- Forecast præcision på forskellige tidspunkter før resolution
        -- Gemmes som JSON: {"24h": 0.72, "12h": 0.85, "6h": 0.91, "1h": 0.95}
        forecast_accuracy_json TEXT
    )""")

    # Forecast-drift tabel — sporer hvordan implied_prob bevæger sig over tid
    # Bruges til at analysere: hvornår opdager markedet ny forecast-info?
    c.execute("""CREATE TABLE IF NOT EXISTS forecast_drift (
        ts INTEGER,                       -- Harvest timestamp
        dt TEXT,
        condition_id TEXT,
        city TEXT,
        resolution_date TEXT,
        hours_until_resolution REAL,      -- Tid tilbage til resolution (timer)
        implied_prob REAL,                -- Markedets ja-sandsynlighed
        yes_bid REAL,
        noaa_forecast_temp_f REAL,        -- NOAA forecast for resolution-tidspunkt
        noaa_forecast_temp_c REAL,
        om_forecast_temp_c REAL,          -- Open-Meteo forecast (alle byer)
        om_forecast_temp_f REAL,
        forecast_in_band INTEGER,         -- 1 hvis forecast falder i temp-band
        edge REAL,                        -- abs(forecast_in_band - implied_prob)
        PRIMARY KEY (ts, condition_id)
    )""")

    # Market discovery log — hvornaar opdagede vi et nyt marked?
    c.execute("""CREATE TABLE IF NOT EXISTS market_discovery_log (
        ts INTEGER,
        dt TEXT,
        condition_id TEXT PRIMARY KEY,
        city TEXT,
        question TEXT,
        temp_low REAL, temp_high REAL,
        resolution_date TEXT,
        initial_implied_prob REAL,        -- Prisen da vi opdagede det
        initial_volume REAL,
        hours_before_resolution REAL      -- Hvor tidligt fandt vi det
    )""")

    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_books_city ON poly_weather_books(city,ts)",
        "CREATE INDEX IF NOT EXISTS idx_books_cid  ON poly_weather_books(condition_id,ts)",
        "CREATE INDEX IF NOT EXISTS idx_noaa_fc    ON noaa_forecast(city,forecast_hour)",
        "CREATE INDEX IF NOT EXISTS idx_om_fc      ON openmeteo_forecast(city,forecast_hour)",
        "CREATE INDEX IF NOT EXISTS idx_obs        ON noaa_observations(city,ts)",
        "CREATE INDEX IF NOT EXISTS idx_drift_cid  ON forecast_drift(condition_id,ts)",
        "CREATE INDEX IF NOT EXISTS idx_drift_city ON forecast_drift(city,resolution_date)",
        "CREATE INDEX IF NOT EXISTS idx_disc_city  ON market_discovery_log(city,resolution_date)",
    ]:
        c.execute(idx)

    conn.commit()
    conn.close()
    log.info(f"Database initialiseret: {DB_PATH}")

# ─── NWS GRID CACHE ───────────────────────────────────────────────────

_nws_grid: dict = {}

def get_nws_grid(city):
    if city in _nws_grid:
        return _nws_grid[city]
    cfg = CITIES[city]
    try:
        r = SESSION.get(f"{NWS_BASE}/points/{cfg['lat']},{cfg['lon']}", timeout=TIMEOUT)
        r.raise_for_status()
        p = r.json()["properties"]
        grid = {
            "office": p["gridId"], "gridX": p["gridX"], "gridY": p["gridY"],
            "hourly_url": p["forecastHourly"],
            "grid_url":   p["forecastGridData"],
        }
        _nws_grid[city] = grid
        log.info(f"  NWS grid: {city} = {grid['office']} {grid['gridX']},{grid['gridY']}")
        return grid
    except Exception as e:
        log.warning(f"  NWS /points fejl {city}: {e}")
        return None

# ─── POLYMARKET WEATHER MARKETS ───────────────────────────────────────

def parse_temp_band(question):
    """
    Parser temperatur-band fra Polymarket weather question.
    Understøtter alle kendte formater inkl. dash: "between 34-37°F", "be 10°C", "be 12°C on"
    """
    import re
    q = question.lower()

    def is_fahrenheit(val):
        return val > 40

    # ── DASH FORMAT: "between X-Y°F" (Polymarket's primære format) ─
    m = re.search(r'between\s+(-?\d+)-(\d+)\s*°?f', q)
    if m: return float(m.group(1)), float(m.group(2))

    m = re.search(r'between\s+(-?\d+)-(\d+)\s*°?c', q)
    if m: return c_to_f(float(m.group(1))), c_to_f(float(m.group(2)))

    # ── "X°F or higher/above" og "X°F or below/lower" ────────────
    # Disse SKAL komme FØR "be X°F" for at undgå fejlmatch
    m = re.search(r'(-?\d+\.?\d*)\s*°?f\s+or\s+(?:higher|above)', q)
    if m: return float(m.group(1)), 999.0

    m = re.search(r'(-?\d+\.?\d*)\s*°?c\s+or\s+(?:higher|above)', q)
    if m: return c_to_f(float(m.group(1))), 999.0

    m = re.search(r'(-?\d+\.?\d*)\s*°?f\s+or\s+(?:below|lower)', q)
    if m: return -999.0, float(m.group(1))

    m = re.search(r'(-?\d+\.?\d*)\s*°?c\s+or\s+(?:below|lower)', q)
    if m: return -999.0, c_to_f(float(m.group(1)))

    # ── EXACT CELSIUS: "be 10°c on" (single degree → ±0.5°C band) ─
    m = re.search(r'\bbe\s+(-?\d+)\s*°c\b', q)
    if m:
        val_c = float(m.group(1))
        return c_to_f(val_c - 0.5), c_to_f(val_c + 0.5)

    # ── EXACT FAHRENHEIT: "be 45°f on" (single degree → ±0.5°F) ──
    m = re.search(r'\bbe\s+(-?\d+)\s*°f\b', q)
    if m:
        val = float(m.group(1))
        return val - 0.5, val + 0.5

    # ── Between X and Y °F (fx "between 75°F and 85°F") ──────────
    m = re.search(r'between\s+(-?\d+\.?\d*)\s*°?f\s+and\s+(-?\d+\.?\d*)\s*°?f', q)
    if m: return float(m.group(1)), float(m.group(2))

    # ── Between X and Y °C ───────────────────────────────────────
    m = re.search(r'between\s+(-?\d+\.?\d*)\s*°?c\s+and\s+(-?\d+\.?\d*)\s*°?c', q)
    if m: return c_to_f(float(m.group(1))), c_to_f(float(m.group(2)))

    # ── Between X and Y degrees ──────────────────────────────────
    m = re.search(r'between\s+(-?\d+\.?\d*)\s+and\s+(-?\d+\.?\d*)\s*degrees?\s*(fahrenheit|celsius)?', q)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        unit = (m.group(3) or "")
        if unit == "celsius": return c_to_f(lo), c_to_f(hi)
        if unit == "fahrenheit" or is_fahrenheit(hi): return lo, hi
        return c_to_f(lo), c_to_f(hi)

    # ── Between X and Y (enhedsløst) ─────────────────────────────
    m = re.search(r'between\s+(-?\d+\.?\d*)\s+and\s+(-?\d+\.?\d*)', q)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        return (lo, hi) if is_fahrenheit(hi) else (c_to_f(lo), c_to_f(hi))

    # ── Above/exceed/reach X°F ────────────────────────────────────
    m = re.search(r'(?:above|over|exceed|reach|higher than)\s+(-?\d+\.?\d*)\s*°?f', q)
    if m: return float(m.group(1)), 999.0

    m = re.search(r'(?:above|over|exceed|reach|higher than)\s+(-?\d+\.?\d*)\s*°?c', q)
    if m: return c_to_f(float(m.group(1))), 999.0

    m = re.search(r'(?:above|over|exceed|reach|higher than)\s+(-?\d+\.?\d*)\s*degrees?', q)
    if m:
        val = float(m.group(1))
        return (val, 999.0) if is_fahrenheit(val) else (c_to_f(val), 999.0)

    # ── Below X°F ────────────────────────────────────────────────
    m = re.search(r'(?:below|under|lower than)\s+(-?\d+\.?\d*)\s*°?f', q)
    if m: return -999.0, float(m.group(1))

    m = re.search(r'(?:below|under|lower than)\s+(-?\d+\.?\d*)\s*°?c', q)
    if m: return -999.0, c_to_f(float(m.group(1)))

    m = re.search(r'(?:below|under|lower than)\s+(-?\d+\.?\d*)\s*degrees?', q)
    if m:
        val = float(m.group(1))
        return (-999.0, val) if is_fahrenheit(val) else (-999.0, c_to_f(val))

    # ── Kolonformat: "highest temp in NYC: above 45°F" ────────────
    if ":" in q:
        after_colon = q.split(":", 1)[1]
        tl, th = parse_temp_band(after_colon)
        if tl is not None:
            return tl, th

    return None, None

def city_from_question(q):
    """
    Find by fra question-tekst.
    Matcher både 'new york', 'nyc', 'new york city' etc.
    Kræver temperature-keyword for at filtrere sports fra.
    """
    ql = q.lower()

    WEATHER_KW = [
        "temperature", "degrees", "°f", "°c", "fahrenheit", "celsius",
        "high", "low", "above", "below", "between", "weather",
        "warm", "cold", "reach", "exceed", "hotter", "cooler", "highest"
    ]
    if not any(kw in ql for kw in WEATHER_KW):
        return None

    # Udvid by-matching med aliasser
    CITY_ALIASES = {
        "NYC":      ["new york", "nyc", "new york city"],
        "Chicago":  ["chicago"],
        "Dallas":   ["dallas"],
        "Seattle":  ["seattle"],
        "Atlanta":  ["atlanta"],
        "Miami":    ["miami"],
        "Toronto":  ["toronto"],
        "London":   ["london"],
    }
    for city, aliases in CITY_ALIASES.items():
        if any(alias in ql for alias in aliases):
            return city
    return None

def parse_res_date(question, end_date):
    import re
    months = {
        "january":"01","february":"02","march":"03","april":"04","may":"05","june":"06",
        "july":"07","august":"08","september":"09","october":"10","november":"11","december":"12",
        "jan":"01","feb":"02","mar":"03","apr":"04","jun":"06","jul":"07",
        "aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
    }
    q = question.lower()
    for mn, mm in months.items():
        m = re.search(rf'\b{mn}\s+(\d{{1,2}})\b', q)
        if m:
            day = int(m.group(1))
            yr  = datetime.datetime.now(datetime.timezone.utc).year
            return f"{yr}-{mm}-{day:02d}"
    return end_date[:10] if end_date and len(end_date) >= 10 else None

def fetch_weather_markets():
    """
    Henter weather temperature markets fra Polymarket.
    Bruger /events/pagination?tag_slug= — det korrekte endpoint.
    """
    markets = []
    seen = set()

    def extract_markets(events):
        added = 0
        for ev in (events or []):
            for m in ev.get("markets", []):
                cid = m.get("conditionId") or m.get("id", "")
                if cid and cid not in seen:
                    seen.add(cid)
                    markets.append(m)
                    added += 1
        return added

    # ── Metode 1: events/pagination med kendte weather tag slugs ──
    weather_tag_slugs = ["weather", "temperature", "climate", "daily-temperature",
                         "weather-forecast", "temp", "meteorology"]
    for tag_slug in weather_tag_slugs:
        try:
            r = SESSION.get(f"{GAMMA_BASE}/events/pagination",
                params={"limit": 100, "active": "true", "archived": "false",
                        "closed": "false", "tag_slug": tag_slug,
                        "order": "volume24hr", "ascending": "false", "offset": 0},
                timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json()
            # Pagination response: {"data": [...], "count": N} eller direkte liste
            events = data.get("data", data) if isinstance(data, dict) else data
            n = extract_markets(events)
            if n:
                log.info(f"  tag_slug='{tag_slug}': {n} markets")
                # Hent eventuelle næste sider
                offset = 100
                while n == 100:
                    r2 = SESSION.get(f"{GAMMA_BASE}/events/pagination",
                        params={"limit": 100, "active": "true", "archived": "false",
                                "closed": "false", "tag_slug": tag_slug,
                                "order": "volume24hr", "ascending": "false", "offset": offset},
                        timeout=TIMEOUT)
                    if r2.status_code != 200: break
                    data2 = r2.json()
                    events2 = data2.get("data", data2) if isinstance(data2, dict) else data2
                    n = extract_markets(events2)
                    offset += 100
            time.sleep(0.2)
        except Exception as e:
            log.debug(f"  tag_slug={tag_slug}: {e}")

    # ── Metode 2: Direkte event slug-opslag per by+dato ───────────
    if not markets:
        log.info("  tag_slug gav 0 — prøver direkte event slug-opslag...")
        city_slug_map = {
            "NYC":     ["nyc", "new-york-city"],
            "Chicago": ["chicago"], "Dallas": ["dallas"], "Seattle": ["seattle"],
            "Atlanta": ["atlanta"], "Miami":  ["miami"],  "Toronto": ["toronto"],
            "London":  ["london"],
        }
        month_names = ["january","february","march","april","may","june",
                       "july","august","september","october","november","december"]
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        for days_ahead in range(-2, 14):
            target = now_utc + datetime.timedelta(days=days_ahead)
            date_str = f"{month_names[target.month-1]}-{target.day}-{target.year}"
            for city, city_slugs in city_slug_map.items():
                for city_slug in city_slugs:
                    slug = f"highest-temperature-in-{city_slug}-on-{date_str}"
                    try:
                        r = SESSION.get(f"{GAMMA_BASE}/events",
                            params={"slug": slug}, timeout=TIMEOUT)
                        if r.status_code == 200:
                            data = r.json()
                            evs = data if isinstance(data, list) else [data]
                            n = extract_markets(evs)
                            if n: break  # Fandt den — næste by
                        time.sleep(0.05)
                    except Exception as e:
                        log.debug(f"  event slug {slug}: {e}")

    if markets:
        sample = markets[0].get("question","") or markets[0].get("title","")
        log.info(f"  fetch_weather_markets: {len(markets)} markeder — sample: '{sample[:80]}'")
    else:
        log.warning("  fetch_weather_markets: 0 markeder fundet — tjek tag_slug i Railway logs")

    return markets


def save_weather_markets(conn, markets):
    ts, dt, saved = now_ts(), now_dt(), 0
    skipped_no_city = 0
    for m in markets:
        # Polymarket kan bruge question, title eller description
        question = (m.get("question") or m.get("title") or m.get("description") or "").strip()
        city = city_from_question(question)
        if not city:
            skipped_no_city += 1
            if skipped_no_city <= 3:
                log.info(f"    Skip (ingen match): '{question[:100]}'")
            continue

        cid    = m.get("conditionId") or m.get("id", "")
        tokens = m.get("clobTokenIds", [])
        if isinstance(tokens, str): tokens = json.loads(tokens)
        outcomes = m.get("outcomes", [])
        if isinstance(outcomes, str): outcomes = json.loads(outcomes)

        yes_tok = no_tok = None
        if len(tokens) >= 2:
            out0 = (outcomes[0] if outcomes else "").lower()
            if any(kw in out0 for kw in ["yes","true"]):
                yes_tok, no_tok = tokens[0], tokens[1]
            else:
                yes_tok, no_tok = tokens[1], tokens[0]
        elif tokens:
            yes_tok = tokens[0]

        tl_f, th_f = parse_temp_band(question)
        end_date   = m.get("endDate") or ""
        res_date   = parse_res_date(question, end_date)

        try:
            conn.execute("""INSERT OR REPLACE INTO poly_weather_markets
                (ts,dt,city,market_slug,condition_id,question,
                 temp_low,temp_high,temp_low_c,temp_high_c,
                 resolution_date,end_date,yes_token_id,no_token_id,
                 active,volume,liquidity)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                ts, dt, city, m.get("slug",""), cid, question,
                tl_f, th_f, f_to_c(tl_f), f_to_c(th_f),
                res_date, end_date[:19] if end_date else None,
                yes_tok, no_tok, 1,
                float(m.get("volume",0) or 0),
                float(m.get("liquidity",0) or 0),
            ))
            saved += 1
        except Exception as e:
            log.warning(f"    Market save fejl: {e}")
    conn.commit()
    if skipped_no_city > 0:
        log.info(f"  save_weather_markets: {saved} gemt, {skipped_no_city} sprunget over (ingen by/weather match)")
    return saved

# ─── POLYMARKET CLOB ORDERBOOKS ───────────────────────────────────────

def fetch_clob_book(token_id):
    try:
        r = SESSION.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        bids = sorted(data.get("bids",[]), key=lambda x: float(x.get("price",0)), reverse=True)
        asks = sorted(data.get("asks",[]), key=lambda x: float(x.get("price",0)))
        bb = float(bids[0]["price"]) if bids else None
        ba = float(asks[0]["price"]) if asks else None
        return {
            "bid": bb, "ask": ba,
            "mid": round((bb+ba)/2,4) if bb and ba else None,
            "bid_size": float(bids[0]["size"]) if bids else 0,
            "ask_size": float(asks[0]["size"]) if asks else 0,
        }
    except Exception as e:
        log.debug(f"    CLOB fejl {token_id[:12]}: {e}")
        return None

def harvest_orderbooks(conn):
    ts, dt = now_ts(), now_dt()
    rows = conn.execute("""
        SELECT condition_id, city, yes_token_id, no_token_id
        FROM poly_weather_markets WHERE active=1 AND yes_token_id IS NOT NULL
    """).fetchall()

    if not rows:
        log.warning("  Ingen aktive weather markets i DB")
        return

    saved, city_c = 0, {}
    for cid, city, yes_tok, no_tok in rows:
        yb = fetch_clob_book(yes_tok) if yes_tok else None
        nb = fetch_clob_book(no_tok)  if no_tok  else None
        if not yb and not nb: continue

        def g(book, k, default=None): return book.get(k, default) if book else default

        yb_p = g(yb,"bid"); ya_p = g(yb,"ask"); ym_p = g(yb,"mid")
        nb_p = g(nb,"bid"); na_p = g(nb,"ask"); nm_p = g(nb,"mid")

        try:
            conn.execute("""INSERT OR REPLACE INTO poly_weather_books
                (ts,dt,city,condition_id,
                 yes_bid,yes_ask,yes_mid,yes_bid_size,yes_ask_size,
                 no_bid,no_ask,no_mid,no_bid_size,no_ask_size,
                 sum_bid,spread_yes,spread_no,implied_prob)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                ts, dt, city, cid,
                yb_p, ya_p, ym_p, g(yb,"bid_size",0), g(yb,"ask_size",0),
                nb_p, na_p, nm_p, g(nb,"bid_size",0), g(nb,"ask_size",0),
                round(yb_p+nb_p,4) if yb_p and nb_p else None,
                round(ya_p-yb_p,4) if ya_p and yb_p else None,
                round(na_p-nb_p,4) if na_p and nb_p else None,
                ym_p,
            ))
            saved += 1
            city_c[city] = city_c.get(city,0) + 1
        except Exception as e:
            log.warning(f"    Book save fejl ({city}): {e}")
        time.sleep(0.1)

    conn.commit()
    for city, n in sorted(city_c.items()):
        log.info(f"  {city}: {n} markets booket")
    log.info(f"  Orderbooks: {saved} snapshots gemt")

# ─── NOAA / NWS ───────────────────────────────────────────────────────

def harvest_noaa_forecast(conn, city):
    ts, dt = now_ts(), now_dt()
    grid = get_nws_grid(city)
    if not grid: return
    try:
        r = SESSION.get(grid["hourly_url"], timeout=TIMEOUT)
        r.raise_for_status()
        periods = r.json()["properties"]["periods"]
    except Exception as e:
        log.warning(f"  NWS hourly fejl ({city}): {e}")
        return

    rows = []
    for p in periods[:24]:
        tf = p.get("temperature")
        ws = p.get("windSpeed","").replace(" mph","").split(" to ")
        ws_val = float(ws[-1]) if ws and ws[-1].replace(".","").isdigit() else None
        precip = p.get("probabilityOfPrecipitation",{})
        if isinstance(precip, dict): precip = precip.get("value")
        hum = p.get("relativeHumidity",{})
        if isinstance(hum, dict): hum = hum.get("value")
        rows.append((ts, dt, city, p["startTime"], tf, f_to_c(tf),
                     None, hum, ws_val, p.get("windDirection"), precip,
                     p.get("shortForecast","")))

    conn.executemany("""INSERT OR REPLACE INTO noaa_forecast
        (ts,dt,city,forecast_hour,temp_f,temp_c,dew_point_f,humidity,
         wind_speed,wind_direction,precip_prob,short_forecast)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    log.info(f"  {city} NOAA forecast: {len(rows)} timer gemt")

def harvest_noaa_griddata(conn, city):
    ts, dt = now_ts(), now_dt()
    grid = get_nws_grid(city)
    if not grid: return
    try:
        r = SESSION.get(grid["grid_url"], timeout=TIMEOUT)
        r.raise_for_status()
        props = r.json()["properties"]
    except Exception as e:
        log.warning(f"  NWS griddata fejl ({city}): {e}")
        return

    def series(key):
        return {v["validTime"].split("/")[0]: v["value"]
                for v in props.get(key,{}).get("values",[])}

    temp_s   = series("temperature")
    dew_s    = series("dewpoint")
    maxT_s   = series("maxTemperature")
    minT_s   = series("minTemperature")
    precip_s = series("quantitativePrecipitation")
    snow_s   = series("snowfallAmount")
    sky_s    = series("skyCover")
    ws_s     = series("windSpeed")
    wg_s     = series("windGust")
    wd_s     = series("windDirection")

    all_times = sorted(set(list(temp_s.keys())[:24]))
    rows = [(ts, dt, city, vt, temp_s.get(vt), dew_s.get(vt),
             maxT_s.get(vt), minT_s.get(vt), precip_s.get(vt),
             snow_s.get(vt), sky_s.get(vt), ws_s.get(vt),
             wg_s.get(vt), wd_s.get(vt)) for vt in all_times]

    if rows:
        conn.executemany("""INSERT OR REPLACE INTO noaa_griddata
            (ts,dt,city,valid_time,temp_c,dew_point_c,max_temp_c,min_temp_c,
             precip_mm,snow_mm,sky_cover,wind_speed_kmh,wind_gust_kmh,wind_dir)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
        conn.commit()
        log.info(f"  {city} NOAA griddata: {len(rows)} perioder gemt")

def harvest_noaa_observations(conn, city):
    ts, dt = now_ts(), now_dt()
    station = CITIES[city]["noaa_station"]
    try:
        r = SESSION.get(f"{NWS_BASE}/stations/{station}/observations/latest", timeout=TIMEOUT)
        r.raise_for_status()
        props = r.json()["properties"]
    except Exception as e:
        log.warning(f"  NWS obs fejl ({city}/{station}): {e}")
        return

    def val(k):
        v = props.get(k)
        return v.get("value") if isinstance(v, dict) else v

    tc = val("temperature"); tf = c_to_f(tc)
    dc = val("dewpoint");    df = c_to_f(dc)
    ws_ms = val("windSpeed");     ws_mph = round(ws_ms*2.237,2)  if ws_ms else None
    wg_ms = val("windGust");      wg_mph = round(wg_ms*2.237,2)  if wg_ms else None
    pres  = val("barometricPressure")
    pres_mb = round(pres/100,2) if pres else None
    hum = round(100*(1-(tc-dc)/25),1) if tc is not None and dc is not None else None
    wx = props.get("presentWeather",[])
    wx_desc = ", ".join([w.get("weather","") for w in wx if w.get("weather")]) if wx else ""

    try:
        conn.execute("""INSERT OR REPLACE INTO noaa_observations
            (ts,dt,city,obs_time,temp_f,temp_c,dew_point_f,humidity,
             wind_speed,wind_direction,wind_gust,pressure_mb,
             visibility_m,precip_1h,weather_desc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            ts, dt, city, props.get("timestamp"), tf, tc, df, hum,
            ws_mph, val("windDirection"), wg_mph, pres_mb,
            val("visibility"), val("precipitationLastHour"), wx_desc
        ))
        conn.commit()
        log.info(f"  {city} obs: {f'{tf:.1f}F' if tf else '?'} ({f'{tc:.1f}C' if tc else '?'})  {wx_desc or 'clear'}")
    except Exception as e:
        log.warning(f"  NOAA obs save fejl ({city}): {e}")

# ─── OPEN-METEO ───────────────────────────────────────────────────────

def harvest_open_meteo(conn, city):
    ts, dt = now_ts(), now_dt()
    cfg = CITIES[city]
    try:
        r = SESSION.get(f"{OM_BASE}/forecast", params={
            "latitude": cfg["lat"], "longitude": cfg["lon"],
            "current": "temperature_2m,apparent_temperature,relative_humidity_2m,"
                       "precipitation,wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
                       "surface_pressure,cloud_cover,weather_code",
            "hourly": "temperature_2m,apparent_temperature,dew_point_2m,"
                      "relative_humidity_2m,precipitation_probability,precipitation,"
                      "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
                      "surface_pressure,cloud_cover,visibility,weather_code",
            "forecast_days": 3,
            "temperature_unit": "celsius", "wind_speed_unit": "kmh",
            "precipitation_unit": "mm", "timezone": cfg["timezone"],
        }, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"  Open-Meteo fejl ({city}): {e}")
        return

    # Current conditions
    cur = data.get("current", {})
    tc = cur.get("temperature_2m")
    try:
        conn.execute("""INSERT OR REPLACE INTO openmeteo_current
            (ts,dt,city,obs_time,temp_c,temp_f,apparent_temp_c,humidity,precip,
             wind_speed,wind_direction,wind_gusts,surface_pressure,cloud_cover,weather_code)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            ts, dt, city, cur.get("time"),
            tc, c_to_f(tc), cur.get("apparent_temperature"),
            cur.get("relative_humidity_2m"), cur.get("precipitation"),
            cur.get("wind_speed_10m"), cur.get("wind_direction_10m"),
            cur.get("wind_gusts_10m"), cur.get("surface_pressure"),
            cur.get("cloud_cover"), cur.get("weather_code"),
        ))
        log.info(f"  {city} Open-Meteo: {tc}C ({c_to_f(tc)}F)")
    except Exception as e:
        log.warning(f"  Open-Meteo current save fejl ({city}): {e}")

    # Hourly forecast
    h = data.get("hourly", {})
    times = h.get("time", [])
    rows = []
    for i, t in enumerate(times[:48]):
        def hv(k): vals=h.get(k,[]); return vals[i] if i<len(vals) else None
        tc_h = hv("temperature_2m")
        rows.append((ts, dt, city, t, tc_h, c_to_f(tc_h),
                     hv("apparent_temperature"), hv("dew_point_2m"),
                     hv("relative_humidity_2m"), hv("precipitation_probability"),
                     hv("precipitation"), hv("wind_speed_10m"),
                     hv("wind_direction_10m"), hv("wind_gusts_10m"),
                     hv("surface_pressure"), hv("cloud_cover"),
                     hv("visibility"), hv("weather_code")))

    if rows:
        conn.executemany("""INSERT OR REPLACE INTO openmeteo_forecast
            (ts,dt,city,forecast_hour,temp_c,temp_f,apparent_temp_c,dew_point_c,
             humidity,precip_prob,precipitation,wind_speed,wind_direction,wind_gusts,
             surface_pressure,cloud_cover,visibility,weather_code)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
        conn.commit()
        log.info(f"  {city} Open-Meteo forecast: {len(rows)} timer gemt")

# ─── DOWNLOAD SERVER ──────────────────────────────────────────────────

def start_download_server():
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args): pass
        def do_GET(self):
            if self.path == "/db":
                if not DB_PATH.exists():
                    self.send_response(404); self.end_headers()
                    self.wfile.write(b"Database ikke klar"); return
                buf = io.BytesIO()
                with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
                    zf.write(DB_PATH, "weather_harvest.db")
                data = buf.getvalue()
                self.send_response(200)
                self.send_header("Content-Type","application/zip")
                self.send_header("Content-Disposition","attachment; filename=weather_harvest.db.zip")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers(); self.wfile.write(data)
            elif self.path == "/":
                sz = DB_PATH.stat().st_size/1024/1024 if DB_PATH.exists() else 0
                body = f"<html><body><h2>Weather Harvester</h2><p>DB: {sz:.2f} MB</p><p><a href='/db'>Download</a></p></body></html>".encode()
                self.send_response(200)
                self.send_header("Content-Type","text/html")
                self.end_headers(); self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
    port = int(os.environ.get("PORT", 8080))
    http.server.HTTPServer(("0.0.0.0", port), Handler).serve_forever()

# ─── STATUS + EXPORT ──────────────────────────────────────────────────

def show_status():
    if not DB_PATH.exists():
        print("Ingen database endnu."); return
    conn = sqlite3.connect(DB_PATH)
    print(f"\nDatabase: {DB_PATH} ({DB_PATH.stat().st_size/1024/1024:.2f} MB)")

    print("\n── Polymarket markets ─────────────────")
    for r in conn.execute("SELECT city,COUNT(*),MIN(resolution_date),MAX(resolution_date) FROM poly_weather_markets WHERE active=1 GROUP BY city ORDER BY city").fetchall():
        print(f"  {r[0]:10} {r[1]:3} markets ({r[2]} → {r[3]})")

    print("\n── Orderbook snapshots ────────────────")
    for r in conn.execute("SELECT city,COUNT(*),MAX(dt) FROM poly_weather_books GROUP BY city ORDER BY city").fetchall():
        print(f"  {r[0]:10} {r[1]:5} snapshots  (seneste: {r[2]})")

    print("\n── NOAA seneste observationer ─────────")
    for r in conn.execute("""
        SELECT n.city, n.temp_f, n.temp_c, n.humidity, n.weather_desc, n.dt
        FROM noaa_observations n
        JOIN (SELECT city, MAX(ts) mts FROM noaa_observations GROUP BY city) m
        ON n.city=m.city AND n.ts=m.mts ORDER BY n.city""").fetchall():
        print(f"  {r[0]:10} {f'{r[1]:.1f}F' if r[1] else '?':8} ({f'{r[2]:.1f}C' if r[2] else '?':6})  {f'{r[3]:.0f}%' if r[3] else '?':5}  {r[4] or 'clear'}")

    print("\n── Open-Meteo seneste ─────────────────")
    for r in conn.execute("""
        SELECT n.city, n.temp_c, n.temp_f, n.humidity, n.dt
        FROM openmeteo_current n
        JOIN (SELECT city, MAX(ts) mts FROM openmeteo_current GROUP BY city) m
        ON n.city=m.city AND n.ts=m.mts ORDER BY n.city""").fetchall():
        print(f"  {r[0]:10} {f'{r[1]:.1f}C' if r[1] else '?':8} ({f'{r[2]:.1f}F' if r[2] else '?':6})")
    conn.close()

def export_csv():
    if not DB_PATH.exists():
        print("Ingen database."); return
    Path("exports").mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    tables = {
        "exports/weather_markets.csv":    "SELECT * FROM poly_weather_markets",
        "exports/orderbooks.csv":         "SELECT * FROM poly_weather_books ORDER BY ts",
        "exports/noaa_forecast.csv":      "SELECT * FROM noaa_forecast ORDER BY city,ts",
        "exports/noaa_observations.csv":  "SELECT * FROM noaa_observations ORDER BY city,ts",
        "exports/noaa_griddata.csv":      "SELECT * FROM noaa_griddata ORDER BY city,ts",
        "exports/openmeteo_forecast.csv": "SELECT * FROM openmeteo_forecast ORDER BY city,ts",
        "exports/openmeteo_current.csv":  "SELECT * FROM openmeteo_current ORDER BY city,ts",
    }
    for path, q in tables.items():
        c = conn.execute(q); rows = c.fetchall(); h = [d[0] for d in c.description]
        with open(path,"w",newline="") as f:
            w = csv.writer(f); w.writerow(h); w.writerows(rows)
        print(f"  {path} ({len(rows):,} raekker)")
    conn.close()
    print("Alle CSV'er gemt i exports/")



# ─── FORECAST DRIFT TRACKING ──────────────────────────────────────────

def _get_forecast_for_date(conn, city, resolution_date, source):
    """
    Henter forventet MAX temperatur for resolution_date for en given by.
    Bruger seneste forecast (højeste ts) der dækker den pågældende dato.
    Returnerer (temp_f, temp_c) eller (None, None).
    """
    if source == "noaa":
        # Brug MAX forecast temp over alle timer på resolution_date
        # forecast_hour format: '2026-03-08T14:00:00-08:00' → substr(1,10) = '2026-03-08'
        row = conn.execute("""
            SELECT MAX(temp_f), MAX(temp_c) FROM noaa_forecast
            WHERE city = ?
              AND substr(forecast_hour, 1, 10) = ?
        """, (city, resolution_date)).fetchone()
        if row and row[0] is not None:
            return row[0], row[1]

        # Fallback: griddata max temp
        row = conn.execute("""
            SELECT MAX(max_temp_c) FROM noaa_griddata
            WHERE city = ?
              AND substr(valid_time, 1, 10) = ?
        """, (city, resolution_date)).fetchone()
        if row and row[0] is not None:
            return c_to_f(row[0]), row[0]

    else:  # open_meteo
        # Open-Meteo tabel hedder openmeteo_forecast eller open_meteo_forecast
        for tbl in ("openmeteo_forecast", "open_meteo_forecast", "om_forecast"):
            try:
                row = conn.execute(f"""
                    SELECT MAX(temp_f), MAX(temp_c) FROM {tbl}
                    WHERE city = ?
                      AND substr(forecast_hour, 1, 10) = ?
                """, (city, resolution_date)).fetchone()
                if row and row[0] is not None:
                    return row[0], row[1]
            except Exception:
                continue

    return None, None

def harvest_forecast_drift(conn):
    """
    For hvert aktivt market: beregn og gem:
    - Nuvaerende implied probability (fra seneste orderbook)
    - NOAA/OM forecast for resolution-dagen
    - Om forecast falder i temp-band (edge signal)
    - Timer tilbage til resolution

    Dette er kernedata til backtesting af forecast-arbitrage strategier.
    """
    ts, dt = now_ts(), now_dt()

    # Hent alle aktive markets med resolution_date
    rows = conn.execute("""
        SELECT m.condition_id, m.city, m.resolution_date,
               m.temp_low, m.temp_high,
               b.implied_prob, b.yes_bid
        FROM poly_weather_markets m
        LEFT JOIN (
            SELECT condition_id, implied_prob, yes_bid
            FROM poly_weather_books
            WHERE ts = (SELECT MAX(ts) FROM poly_weather_books b2
                        WHERE b2.condition_id = poly_weather_books.condition_id)
        ) b ON m.condition_id = b.condition_id
        WHERE m.active = 1 AND m.resolution_date IS NOT NULL
    """).fetchall()

    if not rows:
        return

    drift_rows = []
    for cid, city, res_date, tl_f, th_f, implied, yes_bid in rows:
        # Timer til resolution
        hours_left = None
        if res_date:
            try:
                res_dt = datetime.datetime.strptime(res_date, "%Y-%m-%d").replace(
                    tzinfo=datetime.timezone.utc)
                hours_left = round(
                    (res_dt - datetime.datetime.now(datetime.timezone.utc)
                    ).total_seconds() / 3600, 1)
            except Exception:
                pass

        # Hent forecast for resolution-dagen
        cfg = CITIES.get(city, {})
        src = cfg.get("source", "noaa")

        noaa_tf = noaa_tc = om_tc = om_tf = None

        if src == "noaa":
            noaa_tf, noaa_tc = _get_forecast_for_date(conn, city, res_date, "noaa")
            # Hent ogsaa Open-Meteo som second opinion for US byer
            om_tc_row = conn.execute("""
                SELECT temp_c, temp_f FROM openmeteo_forecast
                WHERE city = ? AND forecast_hour LIKE ?
                ORDER BY ts DESC LIMIT 1
            """, (city, (res_date or "") + "%")).fetchone()
            if om_tc_row:
                om_tc, om_tf = om_tc_row
        else:
            om_tf, om_tc = _get_forecast_for_date(conn, city, res_date, "open_meteo")

        # Er forecast temperaturen inden for temp-band?
        forecast_in_band = None
        best_fc_f = noaa_tf or om_tf
        if best_fc_f is not None and tl_f is not None and th_f is not None:
            in_band = (tl_f <= best_fc_f <= th_f) if th_f < 900 else (best_fc_f >= tl_f)
            forecast_in_band = 1 if in_band else 0

        # Edge: afstand mellem forecast-sandsynlighed og market-pris
        edge = None
        if forecast_in_band is not None and implied is not None:
            edge = round(abs(forecast_in_band - implied), 4)

        drift_rows.append((
            ts, dt, cid, city, res_date, hours_left,
            implied, yes_bid,
            noaa_tf, noaa_tc, om_tc, om_tf,
            forecast_in_band, edge,
        ))

    if drift_rows:
        conn.executemany(
            "INSERT OR REPLACE INTO forecast_drift "
            "(ts,dt,condition_id,city,resolution_date,hours_until_resolution,"
            " implied_prob,yes_bid,"
            " noaa_forecast_temp_f,noaa_forecast_temp_c,"
            " om_forecast_temp_c,om_forecast_temp_f,"
            " forecast_in_band,edge)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            drift_rows
        )
        conn.commit()
        # Kun aktive markets (hours_until_resolution > -6) i edge-logningen
        high_edge = [(r[3], r[13]) for r in drift_rows
                     if r[13] and r[13] > 0.3 and r[5] is not None and r[5] > -6]
        log.info(f"  Forecast drift: {len(drift_rows)} markets sporet"
                 + (f" | {len(high_edge)} med edge>30%" if high_edge else ""))
        if high_edge:
            for city_e, edge_e in sorted(high_edge, key=lambda x: -x[1])[:3]:
                log.info(f"    EDGE: {city_e} edge={edge_e:.2f}")

# ─── MARKET DISCOVERY TRACKING ────────────────────────────────────────

def log_new_markets(conn, markets, ts, dt):
    """Registrerer nyopdagede markets i discovery_log."""
    known = {r[0] for r in conn.execute(
        "SELECT condition_id FROM market_discovery_log").fetchall()}
    new_count = 0
    for m in markets:
        question = m.get("question","")
        city = city_from_question(question)
        if not city: continue
        cid = m.get("conditionId") or m.get("id","")
        if cid in known: continue

        tl_f, th_f = parse_temp_band(question)
        end_date   = m.get("endDate") or ""
        res_date   = parse_res_date(question, end_date)

        hours_left = None
        if res_date:
            try:
                res_dt = datetime.datetime.strptime(res_date, "%Y-%m-%d").replace(
                    tzinfo=datetime.timezone.utc)
                hours_left = round(
                    (res_dt - datetime.datetime.now(datetime.timezone.utc)
                    ).total_seconds() / 3600, 1)
            except Exception:
                pass

        try:
            # Hent nuværende pris fra seneste orderbook snapshot
            book_row = conn.execute("""
                SELECT implied_prob FROM poly_weather_books
                WHERE condition_id = ?
                ORDER BY ts DESC LIMIT 1
            """, (cid,)).fetchone()
            initial_prob = float(book_row[0]) if book_row and book_row[0] is not None else None

            conn.execute(
                "INSERT OR IGNORE INTO market_discovery_log "
                "(ts,dt,condition_id,city,question,temp_low,temp_high,"
                " resolution_date,initial_implied_prob,initial_volume,hours_before_resolution)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (ts, dt, cid, city, question, tl_f, th_f, res_date,
                 initial_prob, float(m.get("volume",0) or 0), hours_left))
            log.info(f"  NYT marked: {city} | {question[:60]} | prob={initial_prob} ({hours_left}h)")
            new_count += 1
        except Exception as e:
            log.warning(f"  Discovery log fejl: {e}")

    if new_count:
        conn.commit()
    return new_count


# ─── OUTCOME RESOLVER ─────────────────────────────────────────────────

def resolve_outcomes(conn):
    """
    For markets der er passeret resolution_date:
    1. Tjek om markedet er lukket/resolved paa Polymarket
    2. Hent den faktiske temperatur fra NOAA/Open-Meteo observationer
    3. Bestem om YES eller NO vandt
    4. Beregn forecast-praecision paa 24h/12h/6h/1h foer resolution
    """
    ts, dt = now_ts(), now_dt()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Find markets hvor resolution_date er passeret og outcome ikke er registreret
    pending = conn.execute("""
        SELECT m.condition_id, m.city, m.resolution_date,
               m.temp_low, m.temp_high, m.yes_token_id, m.market_slug
        FROM poly_weather_markets m
        LEFT JOIN poly_weather_outcomes o ON m.condition_id = o.condition_id
        WHERE m.active = 1
        AND m.resolution_date IS NOT NULL
        AND m.resolution_date <= ?
        AND (o.condition_id IS NULL OR o.resolved_yes IS NULL)
        LIMIT 20
    """, (now_utc.strftime("%Y-%m-%d"),)).fetchall()

    if not pending:
        return

    log.info(f"  Outcome resolver: {len(pending)} markets klar til resolution-check")

    for cid, city, res_date, tl_f, th_f, yes_tok, slug in pending:

        # 1. Tjek market-status paa Polymarket
        market_closed = False
        poly_resolved_yes = None
        try:
            r = SESSION.get(f"{GAMMA_BASE}/markets",
                            params={"slug": slug}, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if data:
                m_data = data[0] if isinstance(data, list) else data
                if not m_data.get("active", True):
                    market_closed = True
                    # Tjek om vi kan finde en winner
                    # Polymarket saetter prices til 1.0 eller 0.0 ved resolution
                    # Vi ser om yes_price er 1.0
                    yes_price = float(m_data.get("outcomePrices", ["0","0"])[0]
                                      if m_data.get("outcomePrices") else 0)
                    if yes_price >= 0.99:
                        poly_resolved_yes = 1
                    elif yes_price <= 0.01:
                        poly_resolved_yes = 0
        except Exception as e:
            log.debug(f"    Outcome market-check fejl ({city}): {e}")

        # 2. Hent faktisk temperatur fra observations-data
        actual_tf = actual_tc = None
        actual_source = None

        # Soeg i NOAA observations for resolution-dato
        cfg = CITIES.get(city, {})
        if cfg.get("source") == "noaa":
            # Find max temp paa resolution-dato fra observationer
            res_start = res_date + " 00:00:00"
            res_end   = res_date + " 23:59:59"
            row = conn.execute("""
                SELECT MAX(temp_f), MAX(temp_c) FROM noaa_observations
                WHERE city = ? AND obs_time BETWEEN ? AND ?
            """, (city, res_start, res_end)).fetchone()
            if row and row[0] is not None:
                actual_tf, actual_tc = row
                actual_source = "noaa_obs"
            else:
                # Fallback: griddata max temp
                row = conn.execute("""
                    SELECT MAX(max_temp_c) FROM noaa_griddata
                    WHERE city = ? AND valid_time LIKE ?
                """, (city, res_date + "%")).fetchone()
                if row and row[0]:
                    actual_tc = row[0]
                    actual_tf = c_to_f(actual_tc)
                    actual_source = "noaa_grid"
        else:
            # Open-Meteo: find max temp paa resolution-dato
            row = conn.execute("""
                SELECT MAX(temp_c), MAX(temp_f) FROM openmeteo_current
                WHERE city = ? AND obs_time LIKE ?
            """, (city, res_date + "%")).fetchone()
            if row and row[0] is not None:
                actual_tc, actual_tf = row
                actual_source = "openmeteo_obs"

        # 3. Bestem outcome
        resolved_yes = poly_resolved_yes  # Stoler paa Polymarket hvis vi har det
        if resolved_yes is None and actual_tf is not None and tl_f is not None:
            # Beregn selv baseret paa faktisk temp
            if th_f and th_f < 900:
                resolved_yes = 1 if (tl_f <= actual_tf <= th_f) else 0
            else:
                resolved_yes = 1 if actual_tf >= tl_f else 0

        # 4. Beregn forecast praecision paa 24h/12h/6h/1h foer resolution
        accuracy_json = None
        if resolved_yes is not None and res_date:
            try:
                res_dt = datetime.datetime.strptime(res_date, "%Y-%m-%d").replace(
                    hour=18, tzinfo=datetime.timezone.utc)  # Antag 18:00 UTC
                accuracy = {}
                for label, hours_back in [("24h",24),("12h",12),("6h",6),("1h",1)]:
                    window_ts_start = int((res_dt - datetime.timedelta(hours=hours_back+1)).timestamp())
                    window_ts_end   = int((res_dt - datetime.timedelta(hours=hours_back-1)).timestamp())
                    row = conn.execute("""
                        SELECT AVG(implied_prob) FROM poly_weather_books
                        WHERE condition_id = ? AND ts BETWEEN ? AND ?
                    """, (cid, window_ts_start, window_ts_end)).fetchone()
                    if row and row[0] is not None:
                        accuracy[label] = round(row[0], 4)
                if accuracy:
                    accuracy_json = json.dumps(accuracy)
            except Exception as e:
                log.debug(f"    Accuracy beregning fejl: {e}")

        # 5. Gem outcome
        try:
            conn.execute("""
                INSERT OR REPLACE INTO poly_weather_outcomes
                (condition_id, city, resolution_date, temp_low, temp_high,
                 resolved_yes, resolution_ts,
                 actual_temp_f, actual_temp_c, actual_source, forecast_accuracy_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (cid, city, res_date, tl_f, th_f, resolved_yes, ts,
                  actual_tf, actual_tc, actual_source, accuracy_json))

            status = "YES" if resolved_yes == 1 else ("NO" if resolved_yes == 0 else "UKENDT")
            log.info(f"  Outcome: {city} {res_date} [{tl_f}F-{th_f}F] → {status}"
                     + (f" (actual: {actual_tf:.1f}F)" if actual_tf else "")
                     + (f" [Polymarket confirmed]" if poly_resolved_yes is not None else ""))
        except Exception as e:
            log.warning(f"  Outcome save fejl ({city}): {e}")

        # Marker markedet som inaktivt:
        # enten Polymarket siger lukket, eller markedet er >36 timer udløbet
        hours_since = (now_utc - datetime.datetime.strptime(
            res_date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        ).total_seconds() / 3600
        if market_closed or hours_since > 36:
            conn.execute("UPDATE poly_weather_markets SET active=0 WHERE condition_id=?", (cid,))
            if resolved_yes is None and hours_since > 168:  # >7 dage, ingen observation
                log.info(f"  Force-lukker {city} {res_date} (>7 dage, ingen observation)")

    conn.commit()

# ─── HOVED-LOOP ───────────────────────────────────────────────────────

def backfill_temp_bands(conn):
    """
    Retroaktivt: genparser temp_low/temp_high for alle markeder hvor det er NULL.
    Opdaterer også forecast_in_band i forecast_drift tabellen.
    Kør én gang efter deploy af fix.
    """
    # ── 1. Fix poly_weather_markets ───────────────────────────────
    missing = conn.execute("""
        SELECT condition_id, question FROM poly_weather_markets
        WHERE temp_low IS NULL OR temp_high IS NULL
    """).fetchall()

    fixed_markets = 0
    unparseable = []
    for cid, question in missing:
        tl_f, th_f = parse_temp_band(question)
        if tl_f is not None:
            conn.execute("""
                UPDATE poly_weather_markets
                SET temp_low=?, temp_high=?, temp_low_c=?, temp_high_c=?
                WHERE condition_id=?
            """, (tl_f, th_f, f_to_c(tl_f), f_to_c(th_f) if th_f < 900 else None, cid))
            fixed_markets += 1
        else:
            unparseable.append(question)

    # Log de første 5 questions vi ikke kan parse — kritisk for debugging
    for q in unparseable[:5]:
        log.info(f"  Backfill UNPARSEABLE: '{q[:120]}'")
    if len(unparseable) > 5:
        log.info(f"  ... og {len(unparseable)-5} flere")

    # ── 2. Fix poly_weather_outcomes ──────────────────────────────
    missing_out = conn.execute("""
        SELECT o.condition_id, o.city, o.resolution_date,
               m.temp_low, m.temp_high, o.actual_temp_f
        FROM poly_weather_outcomes o
        JOIN poly_weather_markets m ON o.condition_id = m.condition_id
        WHERE o.resolved_yes IS NULL AND o.actual_temp_f IS NOT NULL
          AND m.temp_low IS NOT NULL
    """).fetchall()

    fixed_outcomes = 0
    for cid, city, res_date, tl_f, th_f, actual_f in missing_out:
        if tl_f is None or actual_f is None:
            continue
        if th_f > 900:
            resolved_yes = 1 if actual_f >= tl_f else 0
        else:
            resolved_yes = 1 if tl_f <= actual_f <= th_f else 0
        conn.execute("""
            UPDATE poly_weather_outcomes SET resolved_yes=? WHERE condition_id=?
        """, (resolved_yes, cid))
        fixed_outcomes += 1

    # ── 3. Fix forecast_drift: genberegn forecast_in_band ─────────
    fixed_drift = 0
    drift_missing = conn.execute("""
        SELECT f.rowid, f.condition_id, f.noaa_forecast_temp_f, f.om_forecast_temp_f,
               m.temp_low, m.temp_high, f.implied_prob
        FROM forecast_drift f
        JOIN poly_weather_markets m ON f.condition_id = m.condition_id
        WHERE f.forecast_in_band IS NULL
          AND m.temp_low IS NOT NULL
          AND (f.noaa_forecast_temp_f IS NOT NULL OR f.om_forecast_temp_f IS NOT NULL)
        LIMIT 50000
    """).fetchall()

    updates = []
    for rowid, cid, noaa_f, om_f, tl_f, th_f, implied in drift_missing:
        best_f = noaa_f if noaa_f is not None else om_f
        if best_f is None or tl_f is None: continue
        in_band = (tl_f <= best_f <= th_f) if th_f < 900 else (best_f >= tl_f)
        fib = 1 if in_band else 0
        edge = round(abs(fib - implied), 4) if implied is not None else None
        updates.append((fib, edge, rowid))
        fixed_drift += 1

    if updates:
        conn.executemany(
            "UPDATE forecast_drift SET forecast_in_band=?, edge=? WHERE rowid=?",
            updates
        )

    # ── 4. Slet sports-markeder fra ALLE tabeller ─────────────────
    # Identifikation: temp_low IS NULL OG question har ingen weather-keywords
    sports_cids = conn.execute("""
        SELECT condition_id FROM poly_weather_markets
        WHERE temp_low IS NULL
          AND question NOT LIKE '%temperature%'
          AND question NOT LIKE '%degrees%'
          AND question NOT LIKE '%°f%'
          AND question NOT LIKE '%°c%'
          AND question NOT LIKE '%fahrenheit%'
          AND question NOT LIKE '%above%'
          AND question NOT LIKE '%below%'
          AND question NOT LIKE '%between%'
          AND question NOT LIKE '%weather%'
          AND question NOT LIKE '%highest%'
    """).fetchall()

    sports_cid_list = [r[0] for r in sports_cids]
    sports_deleted = 0
    if sports_cid_list:
        placeholders = ",".join("?" * len(sports_cid_list))
        conn.execute(f"DELETE FROM market_discovery_log WHERE condition_id IN ({placeholders})",
                     sports_cid_list)
        conn.execute(f"DELETE FROM forecast_drift WHERE condition_id IN ({placeholders})",
                     sports_cid_list)
        conn.execute(f"DELETE FROM poly_weather_books WHERE condition_id IN ({placeholders})",
                     sports_cid_list)
        conn.execute(f"DELETE FROM poly_weather_markets WHERE condition_id IN ({placeholders})",
                     sports_cid_list)
        sports_deleted = len(sports_cid_list)
        log.info(f"  Backfill: slettet {sports_deleted} sports-markeder fra alle tabeller")

    # ── 5. Fix market_discovery_log: initial_implied_prob ─────────
    discovery_missing = conn.execute("""
        SELECT d.condition_id FROM market_discovery_log d
        WHERE d.initial_implied_prob IS NULL
    """).fetchall()

    fixed_discovery = 0
    for (cid,) in discovery_missing:
        # Hent første orderbook snapshot
        row = conn.execute("""
            SELECT implied_prob FROM poly_weather_books
            WHERE condition_id = ? ORDER BY ts ASC LIMIT 1
        """, (cid,)).fetchone()
        if row and row[0] is not None:
            conn.execute("""
                UPDATE market_discovery_log SET initial_implied_prob=? WHERE condition_id=?
            """, (row[0], cid))
            fixed_discovery += 1

    conn.commit()
    log.info(f"  Backfill: {fixed_markets} markets, {fixed_outcomes} outcomes, "
             f"{fixed_drift} drift rækker, {sports_deleted} sports slettet, "
             f"{fixed_discovery} discovery priser")
    return fixed_markets, fixed_outcomes, fixed_drift

def harvest_once(conn):
    ts, dt = now_ts(), now_dt()
    log.info(f"─── Harvest cycle: {dt} UTC ───")

    # 1. Polymarket weather markets — scanner for nye + opdaterer eksisterende
    log.info("  Polymarket: scanner weather markets...")
    markets = fetch_weather_markets()
    saved_m = save_weather_markets(conn, markets)
    log.info(f"  Polymarket: {len(markets)} hentet, {saved_m} gemt")

    # 2. Orderbooks FØRST — så initial_implied_prob kan hentes ved log_new_markets
    log.info("  Polymarket: henter orderbooks...")
    harvest_orderbooks(conn)

    # 3. Log nye markets — nu med initial_implied_prob fra netop hentede orderbooks
    new_m = log_new_markets(conn, markets, ts, dt)
    if new_m:
        log.info(f"  Polymarket: {new_m} nye markets opdaget")

    # 3. Vejrdata
    for city, cfg in CITIES.items():
        try:
            if cfg["source"] == "noaa":
                harvest_noaa_observations(conn, city)
                harvest_noaa_forecast(conn, city)
                if ts % 1800 < INTERVAL_SEC:
                    harvest_noaa_griddata(conn, city)
            else:
                harvest_open_meteo(conn, city)
        except Exception as e:
            log.error(f"  {city} fejl: {e}", exc_info=True)

    # 4. Outcome resolver — tjek markets der er passeret resolution_date
    #    Koeres FØR forecast_drift så udløbne markets er active=0 inden edge-logning
    try:
        resolve_outcomes(conn)
    except Exception as e:
        log.error(f"  Outcome resolver fejl: {e}", exc_info=True)

    # 5. Forecast drift — spor edge-udvikling over tid for hvert market
    try:
        harvest_forecast_drift(conn)
    except Exception as e:
        log.error(f"  Forecast drift fejl: {e}", exc_info=True)

    log.info("  Cycle komplet")

def run_harvester():
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  WEATHER MARKET HARVESTER STARTET                 ║")
    log.info(f"║  Byer:     {', '.join(CITIES.keys()):<40}║")
    log.info(f"║  Interval: {INTERVAL_SEC}s (5 min)                          ║")
    log.info(f"║  Database: {str(DB_PATH):<40}║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if os.environ.get("ENABLE_DOWNLOAD","").lower() == "true":
        threading.Thread(target=start_download_server, daemon=True).start()

    log.info("  Pre-cacher NWS grid endpoints...")
    for city, cfg in CITIES.items():
        if cfg["source"] == "noaa":
            get_nws_grid(city)
            time.sleep(0.5)

    while True:
        start = time.time()
        conn  = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")

            # Kør backfill første gang (idempotent — gør ingenting hvis alt er OK)
            if not getattr(run_harvester, "_backfill_done", False):
                log.info("  Kører backfill af eksisterende data...")
                backfill_temp_bands(conn)
                run_harvester._backfill_done = True

            harvest_once(conn)
        except KeyboardInterrupt:
            log.info("Stoppet af bruger."); break
        except Exception as e:
            log.error(f"Harvest fejl: {e}", exc_info=True)
        finally:
            if conn:
                try: conn.close()
                except: pass
        elapsed = time.time() - start
        sleep   = max(0, INTERVAL_SEC - elapsed)
        log.info(f"  Naeste cycle om {sleep:.0f}s ...")
        time.sleep(sleep)

# ─── ENTRY POINT ──────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket Weather Harvester")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--once",   action="store_true")
    args = parser.parse_args()

    init_db()

    if   args.status: show_status()
    elif args.export: export_csv()
    elif args.once:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        conn.execute("PRAGMA journal_mode=WAL")
        try:    harvest_once(conn)
        finally: conn.close()
    else:
        run_harvester()
