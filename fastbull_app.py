import os
import datetime
import time
import threading
import re
import json
import sys
import uuid
import signal
import subprocess
from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright

app = Flask(__name__)

# --- ENVIRONMENT VARIABLES & SECRETS ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')

# --- MONGO DATABASE CONFIGURATION ---
# Short timeouts so a slow/unreachable DB can never freeze the scheduler thread.
client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
    socketTimeoutMS=20000,
)
db = client["fastbull_sentiment_db"]

baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]
chart_history_collection = db["session_chart_history"]
lock_collection = db["scheduler_lock"]

# --- SCRAPE SCHEDULING CONFIG ---
WORKER_MODE = "--scrape-worker" in sys.argv   # child process that only runs the browser scrape
RESULT_MARKER = "__SCRAPE_RESULT__:"
SLOT_MINUTE_OFFSET = 1        # scrape at :01, :11, :21, :31, :41, :51
SCRAPE_TIMEOUT_SEC = 100      # hard kill limit for one browser scrape
MAX_ATTEMPTS_PER_SLOT = 3     # retries inside a slot if FastBull returns nothing
RETRY_DELAY_SEC = 15
LEASE_TTL_SEC = 400           # cross-process lock; auto-expires if a holder dies
HOLDER_ID = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
scrape_mutex = threading.Lock()

# --- FULL 28-PAIR MATRIX + GOLD SYMBOL LIST ---
FASTBULL_SYMBOLS = [
    # Majors (7)
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCHF", "USDCAD", "USDJPY",
    # Crosses (21)
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
    # Gold
    "XAUUSD"
]

def clean_symbol_key(key_str):
    """Strips all non-alphanumeric characters (slashes, brackets, hyphens) and lowercases."""
    return re.sub(r'[^a-zA-Z0-9]', '', str(key_str)).lower()

def get_safe_volume(data_dict, primary_key, secondary_key, fallback_value):
    val = data_dict.get(primary_key)
    if val is None:
        val = data_dict.get(secondary_key)
    return float(val) if val is not None else float(fallback_value)

def get_ny_time():
    return datetime.datetime.now(ZoneInfo("America/New_York"))

def load_db_document(collection, doc_id="state_doc"):
    try:
        doc = collection.find_one({"_id": doc_id})
        return doc if doc else {}
    except Exception:
        return {}

def save_db_document(collection, data, doc_id="state_doc"):
    try:
        data["_id"] = doc_id
        collection.replace_one({"_id": doc_id}, data, upsert=True)
    except Exception as e:
        print(f"Database Write Error: {str(e)}")

def get_current_session_details(ny_dt):
    hour = ny_dt.hour
    if 3 <= hour < 8:
        return "LONDON", 3
    elif 8 <= hour < 18:
        return "NEW YORK", 8
    else:
        return "ASIA", 18

def parse_sentiment_text(text):
    """
    Parses text looking for symbol names and percentage values across expanded DOM containers.
    """
    results = {}
    for sym in FASTBULL_SYMBOLS:
        std_key = sym.upper()
        clean_key = clean_symbol_key(sym)
        if len(clean_key) == 6:
            pair_regex = rf"(?:\[)?{clean_key[:3]}[/\\-]?{clean_key[3:]}(?:\])?"
        else:
            pair_regex = clean_key
            
        pattern = rf'{pair_regex}[\s\S]{{0,250}}?(\d+(?:\.\d+)?)%'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            long_pct = float(match.group(1))
            results[std_key] = {"long": long_pct, "short": round(100.0 - long_pct, 1)}
    return results

def fetch_fastbull_client_sentiment():
    """
    Renders FastBull speculative sentiment page using Playwright.
    Target URL: https://www.fastbull.com/speculative-sentiment?textType=1&id=-1
    Intercepts network requests to extract raw API JSON directly.
    """
    url = "https://www.fastbull.com/speculative-sentiment?textType=1&id=-1"
    extracted_api_data = {}

    def handle_response(response):
        """Intercepts FastBull internal backend API payloads."""
        try:
            if any(k in response.url.lower() for k in ["sentiment", "speculative"]) and response.status == 200:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    data = response.json()
                    
                    items = []
                    if isinstance(data, dict):
                        items = data.get("data", []) or data.get("list", []) or data.get("result", [])
                    elif isinstance(data, list):
                        items = data
                        
                    if isinstance(items, dict):
                        items = items.get("list", []) or items.get("items", [])

                    for item in items:
                        if isinstance(item, dict):
                            raw_symbol = item.get("symbol") or item.get("pair") or item.get("name") or item.get("symbolName", "")
                            long_val = item.get("longRate") or item.get("longPercentage") or item.get("long") or item.get("longRatio", None)
                            if raw_symbol and long_val is not None:
                                cleaned = clean_symbol_key(raw_symbol)
                                for sym in FASTBULL_SYMBOLS:
                                    if clean_symbol_key(sym) == cleaned:
                                        l_pct = float(long_val)
                                        extracted_api_data[sym.upper()] = {
                                            "long": l_pct,
                                            "short": round(100.0 - l_pct, 1)
                                        }
        except Exception:
            pass

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            try:
                page = browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
                page.set_default_timeout(30000)
                page.route(
                    "**/*",
                    lambda route: route.abort()
                    if route.request.resource_type in ("image", "media", "font")
                    else route.continue_()
                )
                page.on("response", handle_response)

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as nav_err:
                    print(f"FastBull Navigation Warning: {str(nav_err)} -- inspecting page DOM anyway")

                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

                for attempt in range(8):
                    page.wait_for_timeout(2500)
                    
                    if len(extracted_api_data) >= 15:
                        print(f"FastBull Network Intercept Success: Captured {len(extracted_api_data)} symbols directly from API.")
                        return extracted_api_data

                    combined_text = ""
                    for frame in page.frames:
                        try:
                            txt = frame.locator("body").inner_text(timeout=2000)
                            if txt:
                                combined_text += "\n" + txt
                        except Exception:
                            continue

                    results = parse_sentiment_text(combined_text)
                    merged = {**results, **extracted_api_data}
                    
                    if len(merged) >= 15:
                        print(f"FastBull Scrape Success: Parsed {len(merged)} distinct symbols on attempt {attempt + 1}.")
                        return merged

                print("FastBull Scrape Warning: 0 symbols parsed across all frames after maximum wait.")
                return None
            finally:
                browser.close()
    except Exception as e:
        print(f"FastBull Playwright Exception: {str(e)}")
        return None

def record_chart_snapshot(symbols, current_session_label, current_date_str, ny_now):
    """Computes combined session + daily deltas for trend charting."""
    try:
        stored_baseline = load_db_document(baseline_collection)
        baseline_volumes = stored_baseline.get("volumes", {})
        stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
        daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

        majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
        tracked_assets = majors + ["GOLD"]

        sess_long_delta, sess_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}
        daily_long_delta, daily_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}

        for name, live in symbols.items():
            cleaned_name = clean_symbol_key(name)
            l_long, l_short = float(live.get("long", 0)), float(live.get("short", 0))

            b_val = baseline_volumes.get(name, {})
            d_val = daily_baseline_volumes.get(name, {})
            b_long = get_safe_volume(b_val, "long", "longVolume", l_long)
            b_short = get_safe_volume(b_val, "short", "shortVolume", l_short)
            d_long = get_safe_volume(d_val, "longVolume", "long", l_long)
            d_short = get_safe_volume(d_val, "shortVolume", "short", l_short)

            if cleaned_name == "xauusd":
                sess_long_delta["GOLD"] = (l_long - b_long)
                sess_short_delta["GOLD"] = (l_short - b_short)
                daily_long_delta["GOLD"] = (l_long - d_long)
                daily_short_delta["GOLD"] = (l_short - d_short)
                continue

            if len(cleaned_name) != 6: continue
            base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
            if base in majors and quote in majors:
                sess_long_delta[base] += (l_long - b_long); sess_short_delta[base] += (l_short - b_short)
                sess_long_delta[quote] += (l_short - b_short); sess_short_delta[quote] += (l_long - b_long)
                daily_long_delta[base] += (l_long - d_long); daily_short_delta[base] += (l_short - d_short)
                daily_long_delta[quote] += (l_short - d_short); daily_short_delta[quote] += (l_long - d_long)

        stored_chart = load_db_document(chart_history_collection, "chart_state_doc")
        points = stored_chart.get("points", {}) if stored_chart.get("session") == current_session_label else {}

        timestamp = ny_now.strftime("%H:%M:%S")
        for cur in tracked_assets:
            net_shift = round(sess_long_delta[cur] - sess_short_delta[cur], 2)
            d_net_shift = round(daily_long_delta[cur] - daily_short_delta[cur], 2)
            combined_value = round(net_shift + d_net_shift, 2)
            points.setdefault(cur, []).append({"t": timestamp, "v": combined_value})
            points[cur] = points[cur][-600:]

        save_db_document(chart_history_collection, {
            "session": current_session_label,
            "session_date": current_date_str,
            "points": points
        }, "chart_state_doc")
    except Exception as e:
        print(f"Chart Snapshot Error: {str(e)}")

def is_market_closed(ny_now):
    weekday = ny_now.weekday()
    return (
        weekday == 5 or
        (weekday == 4 and ny_now.hour >= 17) or
        (weekday == 6 and ny_now.hour < 17)
    )

def kill_process_tree(proc):
    """Kills the worker AND the node/chromium children it spawned (no orphaned browsers)."""
    try:
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        else:
            proc.kill()
    except Exception:
        pass

def run_scrape_subprocess():
    """
    Runs the Playwright scrape in a separate process with a HARD timeout.
    A hung browser can no longer freeze the scheduler: it is killed and the slot moves on.
    """
    cmd = [sys.executable, os.path.abspath(__file__), "--scrape-worker"]
    kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True}
    if hasattr(os, "setsid"):
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, **kwargs)
    try:
        out, _ = proc.communicate(timeout=SCRAPE_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        kill_process_tree(proc)
        try:
            proc.communicate(timeout=5)
        except Exception:
            pass
        print(f"FastBull scrape exceeded {SCRAPE_TIMEOUT_SEC}s -- browser killed.")
        return None

    result = None
    for line in (out or "").splitlines():
        if line.startswith(RESULT_MARKER):
            try:
                result = json.loads(line[len(RESULT_MARKER):])
            except ValueError:
                result = None
        elif line.strip():
            print(f"[scrape-worker] {line}")
    return result or None

def execute_scrape_cycle():
    """Performs a single complete scrape, database update, and baseline set."""
    try:
        ny_now = get_ny_time()
        current_date_str = ny_now.strftime("%Y-%m-%d")
        current_session_label, session_anchor_hour = get_current_session_details(ny_now)

        symbols = run_scrape_subprocess()

        if symbols:
            save_db_document(cache_collection, {
                "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                "last_fetch_epoch": ny_now.timestamp(),
                "live_pairs": symbols
            }, "state_doc")

            stored_baseline = load_db_document(baseline_collection)
            session_did_change = (
                stored_baseline.get("active_session") != current_session_label or
                (stored_baseline.get("baseline_date") != current_date_str and current_session_label == "ASIA") or
                not stored_baseline.get("volumes")
            )
            if session_did_change:
                save_db_document(baseline_collection, {
                    "baseline_date": current_date_str,
                    "active_session": current_session_label,
                    "anchor_hour": session_anchor_hour,
                    "volumes": symbols
                }, "state_doc")

            stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
            current_daily_anchor_date = ny_now.strftime("%Y-%m-%d") if ny_now.hour >= 17 else (ny_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

            if not stored_daily_baseline or stored_daily_baseline.get("daily_anchor_date") != current_daily_anchor_date:
                save_db_document(daily_baseline_collection, {
                    "daily_anchor_date": current_daily_anchor_date,
                    "volumes": symbols,
                    "captured_at": ny_now.strftime("%Y-%m-%d %H:%M:%S")
                }, "daily_state_doc")

            record_chart_snapshot(symbols, current_session_label, current_date_str, ny_now)
            return symbols
    except Exception as e:
        print(f"Scrape Cycle Execution Error: {str(e)}")
    return None

# --- CROSS-PROCESS LEASE (prevents duplicate browsers if several workers/instances run) ---
def acquire_lease():
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        lock_collection.find_one_and_update(
            {"_id": "scrape_lock", "$or": [{"expires_at": {"$lt": now}}, {"holder": HOLDER_ID}]},
            {"$set": {"holder": HOLDER_ID, "expires_at": now + datetime.timedelta(seconds=LEASE_TTL_SEC)}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        return True
    except DuplicateKeyError:
        return False   # someone else holds a live lease
    except Exception as e:
        print(f"Lease Acquire Error: {str(e)}")
        return False

def release_lease():
    try:
        lock_collection.update_one(
            {"_id": "scrape_lock", "holder": HOLDER_ID},
            {"$set": {"expires_at": datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)}}
        )
    except Exception:
        pass

def run_scrape_guarded(scheduled=None):
    """
    One scrape slot: single-flight, retried, market-hours aware.
    Returns the symbols dict, or None (closed market / skipped / failed).
    """
    if not scrape_mutex.acquire(blocking=False):
        print("A scrape is already running in this process -- skipping.")
        return None
    try:
        if not acquire_lease():
            print("Another instance holds the scrape lease -- skipping.")
            return None
        try:
            if scheduled is not None:
                last_epoch = load_db_document(cache_collection).get("last_fetch_epoch", 0)
                if last_epoch >= scheduled.timestamp():
                    print("This slot was already fetched by another worker -- skipping.")
                    return None

            for attempt in range(1, MAX_ATTEMPTS_PER_SLOT + 1):
                if is_market_closed(get_ny_time()):
                    print("Weekend pause: Forex market closed, skipping scrape cycle.")
                    return None
                symbols = execute_scrape_cycle()
                if symbols:
                    return symbols
                if attempt < MAX_ATTEMPTS_PER_SLOT:
                    print(f"Scrape attempt {attempt}/{MAX_ATTEMPTS_PER_SLOT} returned nothing -- retrying in {RETRY_DELAY_SEC}s.")
                    time.sleep(RETRY_DELAY_SEC)
            print("All scrape attempts for this slot failed; waiting for next slot.")
            return None
        finally:
            release_lease()
    finally:
        scrape_mutex.release()

# --- BACKGROUND SCHEDULER WORKER ---
def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def next_slot_after(dt):
    """Next :01/:11/:21/... boundary strictly after dt (UTC minutes align with NY: whole-hour offset)."""
    base = dt.replace(second=0, microsecond=0)
    cand = base + datetime.timedelta(minutes=(SLOT_MINUTE_OFFSET - base.minute) % 10)
    if cand <= dt:
        cand += datetime.timedelta(minutes=10)
    return cand

def run_background_state_scheduler():
    """
    Deadline-based loop: every slot is guaranteed to fire (no 60-second window to miss),
    late wake-ups (host suspended/throttled) trigger an immediate catch-up scrape,
    and a stuck scrape can't block the loop thanks to the subprocess hard timeout.
    """
    print("Executing immediate startup scrape cycle...")
    try:
        run_scrape_guarded()
    except Exception as e:
        print(f"Startup Scrape Error: {str(e)}")

    next_run = next_slot_after(utc_now())
    while True:
        try:
            now = utc_now()
            if now >= next_run:
                scheduled = next_run
                lateness = (now - scheduled).total_seconds()
                if lateness > 60:
                    print(f"Scheduler woke {int(lateness)}s late -- running catch-up scrape now.")
                run_scrape_guarded(scheduled)
                now = utc_now()
                # After a late wake-up, don't replay every missed slot: one fresh scrape is enough.
                next_run = next_slot_after(now if lateness > 60 else scheduled)
        except Exception as e:
            print(f"Scheduler Loop Error: {str(e)}")

        sleep_for = (next_run - utc_now()).total_seconds()
        time.sleep(max(0.5, min(5.0, sleep_for)))

# --- THREAD CONTROL & LOCK ---
background_engine_thread = None
scheduler_lock = threading.Lock()

def start_background_scheduler():
    global background_engine_thread
    with scheduler_lock:
        if background_engine_thread is None or not background_engine_thread.is_alive():
            background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
            background_engine_thread.start()
            print("Background scheduler thread started successfully.")

@app.before_request
def ensure_background_engine_running():
    start_background_scheduler()

if not WORKER_MODE:
    start_background_scheduler()

def process_sentiment_matrix():
    ny_now = get_ny_time()
    cached_data = load_db_document(cache_collection)
    live_pairs = {str(k): v for k, v in cached_data.get("live_pairs", {}).items()}
    stored_baseline = load_db_document(baseline_collection)
    baseline_volumes = stored_baseline.get("volumes", {})
    stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
    daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

    majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
    tracked_assets = majors + ["GOLD"]

    abs_long_pct_sum = {asset: 0.0 for asset in tracked_assets}
    abs_pair_counts = {asset: 0 for asset in tracked_assets}
    sess_long_delta, sess_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}
    daily_long_delta, daily_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}

    for name, live in live_pairs.items():
        cleaned_name = clean_symbol_key(name)
        l_long, l_short = float(live.get("long", 0)), float(live.get("short", 0))
        total_live = l_long + l_short

        b_val = baseline_volumes.get(name, {})
        d_val = daily_baseline_volumes.get(name, {})

        b_long = get_safe_volume(b_val, "long", "longVolume", l_long)
        b_short = get_safe_volume(b_val, "short", "shortVolume", l_short)

        d_long = get_safe_volume(d_val, "longVolume", "long", l_long)
        d_short = get_safe_volume(d_val, "shortVolume", "short", l_short)

        if cleaned_name == "xauusd":
            if total_live > 0: abs_long_pct_sum["GOLD"] = (l_long / total_live)
            abs_pair_counts["GOLD"] = 1
            sess_long_delta["GOLD"] = (l_long - b_long)
            sess_short_delta["GOLD"] = (l_short - b_short)
            daily_long_delta["GOLD"] = (l_long - d_long)
            daily_short_delta["GOLD"] = (l_short - d_short)
            continue

        if len(cleaned_name) != 6: continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()

        if base in majors and quote in majors:
            if total_live > 0:
                abs_long_pct_sum[base] += (l_long / total_live); abs_pair_counts[base] += 1
                abs_long_pct_sum[quote] += (l_short / total_live); abs_pair_counts[quote] += 1

            sess_long_delta[base] += (l_long - b_long); sess_short_delta[base] += (l_short - b_short)
            sess_long_delta[quote] += (l_short - b_short); sess_short_delta[quote] += (l_long - b_long)

            daily_long_delta[base] += (l_long - d_long); daily_short_delta[base] += (l_short - d_short)
            daily_long_delta[quote] += (l_short - d_short); daily_short_delta[quote] += (l_long - d_long)

    currency_scores, daily_currency_scores, combined_currency_scores, bias_output = {}, {}, {}, []

    for cur in tracked_assets:
        count = abs_pair_counts[cur]
        inv_long_ratio = (abs_long_pct_sum[cur] / count) if count > 0 else 0.5
        display_name = "Gold" if cur == "GOLD" else cur
        bias_output.append({"currency": display_name, "long_pct": round(inv_long_ratio * 100, 1), "bias_label": "BULLISH" if inv_long_ratio >= 0.5 else "BEARISH"})

        # Active Session Calculation
        net_shift = sess_long_delta[cur] - sess_short_delta[cur]
        formatted_score = round(net_shift, 2)
        status_str = "UP" if formatted_score > 0 else ("DOWN" if formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        currency_scores[cur] = {"currency": display_name, "value": abs(formatted_score), "status": status_str}

        # Daily (24H) Calculation
        d_net_shift = daily_long_delta[cur] - daily_short_delta[cur]
        d_formatted_score = round(d_net_shift, 2)
        d_status_str = "UP" if d_formatted_score > 0 else ("DOWN" if d_formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        daily_currency_scores[cur] = {"currency": display_name, "value": abs(d_formatted_score), "status": d_status_str}

        # Combined (24H + Active Session) Calculation
        c_net_shift = net_shift + d_net_shift
        c_formatted_score = round(c_net_shift, 2)
        c_status_str = "UP" if c_formatted_score > 0 else ("DOWN" if c_formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        combined_currency_scores[cur] = {"currency": display_name, "value": abs(c_formatted_score), "status": c_status_str}

    return {
        "top_4_up": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "bottom_4_down": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "daily_top_4_up": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "daily_bottom_4_down": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "combined_top_4_up": [x for x in sorted(list(combined_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "combined_bottom_4_down": [x for x in sorted(list(combined_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "absolute_bias": sorted(bias_output, key=lambda x: x['long_pct'], reverse=True),
        "ny_time": ny_now.strftime("%I:%M:%S %p"),
        "api_sync_time": cached_data.get("last_fetch_time", "Syncing..."),
        "active_session": get_current_session_details(ny_now)[0],
        "baseline_set_at": f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)" if stored_baseline.get('active_session') else "Init"
    }

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FastBull Speculative Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: -apple-system, sans-serif; margin: 0; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        h1 { margin: 0; font-size: 22px; color: #f59e0b; font-weight: 700; }
        h2 { font-size: 13px; color: #94a3b8; margin-bottom: 15px; text-transform: uppercase; }
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; font-size: 12px; font-weight: 700; background-color: #111827; border: 1px solid #1f2937; color: #475569; }
        .active-session-live { background-color: #78350f; border: 2px solid #f59e0b; color: #fef3c7; }
        .section-split { display: flex; flex-direction: row; gap: 25px; width: 100%; align-items: flex-start; }
        .panel { background-color: #111827; border: 1px solid #1f1f23; border-radius: 10px; padding: 20px; flex: 1; }
        .column-stack { flex: 1; display: flex; flex-direction: column; gap: 25px; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .grid-box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; border-top: 3px solid #374151; }
        .border-up { border-top-color: #10b981; }
        .border-down { border-top-color: #ef4444; }
        .currency-txt { font-size: 16px; font-weight: 700; }
        .value-box { font-size: 18px; font-weight: 800; display: block; margin: 5px 0; }
        .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
        .bias-list { display: flex; flex-direction: column; }
        .data-row { display: flex; align-items: center; padding: 11px 10px; border-bottom: 1px solid #1f2937; gap: 12px; }
        .bar-container { width: 100px; background: #334155; height: 8px; border-radius: 4px; }
        .bar-fill { height: 100%; border-radius: 4px; }
        .chart-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-top: 5px; }
        .chart-box { background-color: #1f2937; border-radius: 8px; padding: 12px; }
        .chart-label { font-size: 13px; font-weight: 700; color: #e2e8f0; margin-bottom: 6px; }
        .chart-canvas-wrap { position: relative; height: 90px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div><h1>FastBull Speculative Sentiment Matrix Terminal</h1><div style="font-size: 12px; color: #64748b;">Active Anchor: {{ data.baseline_set_at }}</div></div>
            <div style="text-align: right; font-size: 12px; color: #64748b;">Sync: {{ data.api_sync_time }}<br>NY Time: {{ data.ny_time }}</div>
        </div>
        
        <div class="session-tracker-bar">
            <div class="session-card {{ 'active-session-live' if data.active_session == 'ASIA' else '' }}">ASIA SESSION OPEN</div>
            <div class="session-card {{ 'active-session-live' if data.active_session == 'LONDON' else '' }}">LONDON SESSION OPEN</div>
            <div class="session-card {{ 'active-session-live' if data.active_session == 'NEW YORK' else '' }}">NEW YORK SESSION OPEN</div>
        </div>

        <div class="section-split">
            <div class="column-stack">
                <div class="panel">
                    <h2>Cumulative 24H Daily Sentiment</h2>
                    <div class="grid-row">{% for item in data.daily_top_4_up %}<div class="grid-box border-up"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #10b981;">{{ item.value }}</span><span class="badge" style="color:#10b981;">UP</span></div>{% endfor %}</div>
                    <div class="grid-row" style="margin-top:10px;">{% for item in data.daily_bottom_4_down %}<div class="grid-box border-down"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #ef4444;">{{ item.value }}</span><span class="badge" style="color:#ef4444;">DOWN</span></div>{% endfor %}</div>
                </div>
                <div class="panel">
                    <h2>Total Combined Sentiment (24H + Session)</h2>
                    <div class="grid-row">{% for item in data.combined_top_4_up %}<div class="grid-box border-up"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #10b981;">{{ item.value }}</span><span class="badge" style="color:#10b981;">UP</span></div>{% endfor %}</div>
                    <div class="grid-row" style="margin-top:10px;">{% for item in data.combined_bottom_4_down %}<div class="grid-box border-down"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #ef4444;">{{ item.value }}</span><span class="badge" style="color:#ef4444;">DOWN</span></div>{% endfor %}</div>
                </div>
            </div>
            <div class="column-stack">
                <div class="panel">
                    <h2>Active Session Value Shifts</h2>
                    <div class="grid-row">{% for item in data.top_4_up %}<div class="grid-box border-up"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #10b981;">{{ item.value }}</span><span class="badge" style="color:#10b981;">UP</span></div>{% endfor %}</div>
                    <div class="grid-row" style="margin-top:10px;">{% for item in data.bottom_4_down %}<div class="grid-box border-down"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #ef4444;">{{ item.value }}</span><span class="badge" style="color:#ef4444;">DOWN</span></div>{% endfor %}</div>
                </div>
                <div class="panel">
                    <h2>Absolute Retail Positioning Bias (Total Inventory)</h2>
                    <div class="bias-list">
                        {% for item in data.absolute_bias %}
                        <div class="data-row">
                            <span class="currency-txt" style="min-width: 50px;">{{ item.currency }}</span>
                            <span style="font-size: 14px; color: #f1f5f9; margin-right: 10px; min-width: 45px; text-align: right;">{{ item.long_pct }}%</span>
                            <div class="bar-container"><div class="bar-fill" style="width: {{ item.long_pct }}%; background-color: {% if item.long_pct >= 50.0 %}#f59e0b{% else %}#38bdf8{% endif %};"></div></div>
                            <span class="badge" style="margin-left: 10px; background: rgba(245, 158, 11, 0.1); color: #f59e0b;">{{ item.bias_label }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>
        </div>

        <div class="panel" style="margin-top: 25px;">
            <h2>Session Sentiment Trend (Daily + Session Combined, resets each new session)</h2>
            <div class="chart-grid">
                {% for cur in ['EUR','GBP','USD','AUD','NZD','CAD','CHF','JPY','GOLD'] %}
                <div class="chart-box">
                    <div class="chart-label">{{ 'Gold' if cur == 'GOLD' else cur }}</div>
                    <div class="chart-canvas-wrap"><canvas id="chart-{{ cur }}"></canvas></div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
    <script>
        (function() {
            function computeSlopeLine(values) {
                var n = values.length;
                if (n < 2) return values.slice();
                var sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
                for (var i = 0; i < n; i++) {
                    sumX += i;
                    sumY += values[i];
                    sumXY += i * values[i];
                    sumXX += i * i;
                }
                var denom = (n * sumXX - sumX * sumX);
                var slope = denom !== 0 ? (n * sumXY - sumX * sumY) / denom : 0;
                var intercept = (sumY - slope * sumX) / n;
                var fitted = [];
                for (var j = 0; j < n; j++) fitted.push(slope * j + intercept);
                return fitted;
            }

            var chartHistory = {{ chart_history | tojson }};
            var currencies = ['EUR','GBP','USD','AUD','NZD','CAD','CHF','JPY','GOLD'];
            currencies.forEach(function(cur) {
                var points = chartHistory[cur] || [];
                var canvas = document.getElementById('chart-' + cur);
                if (!canvas) return;
                var values = points.map(function(p) { return p.v; });
                var labels = points.map(function(p) { return p.t; });
                var slopeLine = computeSlopeLine(values);
                var isUpward = slopeLine.length >= 2 && slopeLine[slopeLine.length - 1] > slopeLine[0];
                var slopeColor = isUpward ? '#10b981' : '#ef4444';
                new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                data: labels.map(function() { return 0; }),
                                borderColor: 'rgba(255, 255, 255, 0.5)',
                                borderWidth: 1,
                                borderDash: [4, 4],
                                pointRadius: 0,
                                fill: false,
                                tension: 0
                            },
                            {
                                data: slopeLine,
                                borderColor: slopeColor,
                                borderWidth: 2,
                                pointRadius: 0,
                                fill: false,
                                tension: 0
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { display: false },
                            y: { grid: { color: '#1f2937' }, ticks: { color: '#64748b', font: { size: 9 } } }
                        }
                    }
                });
            });
        })();
    </script>
    <script>setInterval(function(){ location.reload(); }, 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    try:
        chart_data = load_db_document(chart_history_collection, "chart_state_doc").get("points", {})
        return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix(), chart_history=chart_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/force-scrape')
def force_scrape():
    """Debug route to manually trigger an immediate scrape from browser."""
    symbols = run_scrape_guarded()
    if symbols:
        return jsonify({
            "status": "success",
            "count": len(symbols),
            "symbols": list(symbols.keys()),
            "data": symbols
        })
    return jsonify({"status": "failed", "count": 0}), 500

@app.route('/health')
def health():
    """Lightweight status: also handy as an uptime-pinger target."""
    cached = load_db_document(cache_collection)
    last_epoch = cached.get("last_fetch_epoch")
    return jsonify({
        "status": "ok",
        "last_fetch_time_ny": cached.get("last_fetch_time"),
        "seconds_since_last_fetch": round(time.time() - last_epoch) if last_epoch else None,
        "scheduler_alive": bool(background_engine_thread and background_engine_thread.is_alive())
    })

if WORKER_MODE:
    # Child-process entry point: scrape once, emit JSON on a marker line, exit.
    _result = fetch_fastbull_client_sentiment()
    print(RESULT_MARKER + json.dumps(_result), flush=True)
    sys.exit(0)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
