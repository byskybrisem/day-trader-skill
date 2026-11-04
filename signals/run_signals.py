#!/usr/bin/env python3
"""Virtual buy-signal runner for GitHub Actions (or any cron). Standard library only.

It chains the pack's own scripts, the same way a person would by hand:

  0. clock gate      NYSE calendar + New York time (DST handled by the scripts' own rule):
                     only on trading days, 09:50 to 15:30 ET (12:30 on 1:00 p.m. early closes)
  1. trader-scanner  scan.py universe, then scan.py orb (opening-range breakouts so far today)
  2. trader-entry    entry.py check SYMBOL (the trigger bar, re-read from Yahoo)
     freshness       the trigger bar closed at most --max-signal-age minutes ago and the last
                     completed bar is at most --max-data-lag minutes old; else no signal
  3. trader-news     news.py --hours 24: event risk (earnings, deal, legal, halt ...) or a big move
  4. trader-exit     exit.py plan (stop at the opening-range midpoint, target 2R, flat by 15:55)
  5. trader-budget   size.py (the virtual $1,000 budget, $10 max loss a trade)
  6. trader-risk     check.py, with today's earlier virtual signals in front of it
  7. trader-timemachine  timemachine.py run: 10,000 replays + the rule's own history;
                     average R <= 0 in either, or too little history -> BEKLE (WAIT)
  8. Telegram        only for a new signal that passed everything; each symbol at most once a day

It never places an order and never talks to a broker: trader-broker is not called anywhere.
Exit code 0 on a normal run (signal or not); 2 on bad arguments.
"""

import argparse
import datetime
import importlib.util
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = sys.executable or "python3"

SCRIPTS = {
    "scan": os.path.join(ROOT, "trader-scanner", "scan.py"),
    "entry": os.path.join(ROOT, "trader-entry", "entry.py"),
    "news": os.path.join(ROOT, "trader-news", "news.py"),
    "exit": os.path.join(ROOT, "trader-exit", "exit.py"),
    "size": os.path.join(ROOT, "trader-budget", "size.py"),
    "risk": os.path.join(ROOT, "trader-risk", "check.py"),
    "tm": os.path.join(ROOT, "trader-timemachine", "timemachine.py"),
}

WINDOW_START = datetime.time(9, 50)
WINDOW_END = datetime.time(15, 30)
EARLY_WINDOW_END = datetime.time(12, 30)
BAR_MINUTES = 5

EVENT_TAGS = {"earnings", "deal", "legal", "halt", "risk", "regulatory"}
GENERIC_WORDS = {
    "the", "first", "american", "united", "national", "general", "advanced", "international",
    "global", "new", "western", "southern", "northern", "eastern", "royal", "bank", "federal",
    "great", "public", "capital", "energy", "health", "technologies", "group", "holdings",
}
KEEP_DAYS = 10


def _load_scan():
    spec = importlib.util.spec_from_file_location("trader_scan_for_signals", SCRIPTS["scan"])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SCAN = _load_scan()  # the scanner's own calendar, DST rule and clock parsing


# ---------------------------------------------------------------- clock

def ny_now(now_utc):
    """naive UTC -> naive New York time (the scripts' DST rule)."""
    return SCAN.to_eastern(now_utc)


def gate(now_utc):
    """Is this a moment to look for signals? -> (ok, reason, window_end or None)."""
    local = ny_now(now_utc)
    day = local.date()
    if day.year not in SCAN.CALENDAR_YEARS:
        return False, "calendar_unknown", None
    ok, why = SCAN.trading_day(day)
    if not ok:
        return False, "closed: " + why, None
    end = EARLY_WINDOW_END if day.isoformat() in SCAN.NYSE_EARLY_CLOSE else WINDOW_END
    t = local.time()
    if t < WINDOW_START:
        return False, "before the window (%s ET; it opens 09:50)" % local.strftime("%H:%M"), end
    if t > end:
        return False, "after the window (%s ET; it closed %s)" % (local.strftime("%H:%M"), end.strftime("%H:%M")), end
    return True, "open", end


def parse_hhmm_ampm(text):
    """'9:55 AM' or '13:05' -> datetime.time, or None."""
    if not text:
        return None
    text = text.strip()
    for fmt in ("%I:%M %p", "%H:%M", "%H:%M:%S"):
        try:
            return datetime.datetime.strptime(text, fmt).time()
        except ValueError:
            pass
    return None


def parse_local(text):
    """'2026-11-02 16:00:02' -> naive datetime, or None."""
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.datetime.strptime(text.strip(), fmt)
        except ValueError:
            pass
    return None


def freshness(entry_doc, now_local, max_age, max_lag):
    """A trigger is only a signal while it is fresh. -> (ok, reason, age_minutes)."""
    if entry_doc.get("status") != "triggered":
        return False, "entry status %s" % entry_doc.get("status"), None
    if entry_doc.get("calendar_known") is False:
        return False, "calendar unknown for this date", None
    day = entry_doc.get("date")
    trig = entry_doc.get("trigger") or {}
    ct = parse_hhmm_ampm(trig.get("close_time") or trig.get("time"))
    if not day or ct is None:
        return False, "no trigger time in the entry result", None
    try:
        trig_dt = datetime.datetime.combine(datetime.date.fromisoformat(day), ct)
    except ValueError:
        return False, "bad entry date", None
    if trig_dt.date() != now_local.date():
        return False, "trigger is from %s, not today" % day, None
    age = (now_local - trig_dt).total_seconds() / 60.0
    if age < 0:
        return False, "trigger time is after the clock", age
    if age > max_age:
        return False, "stale: the trigger bar closed %.0f min ago (limit %d)" % (age, max_age), age
    last = entry_doc.get("last") or {}
    lb = parse_hhmm_ampm(last.get("bar_start"))
    if lb is None:
        return False, "no last completed bar", age
    last_end = datetime.datetime.combine(trig_dt.date(), lb) + datetime.timedelta(minutes=BAR_MINUTES)
    lag = (now_local - last_end).total_seconds() / 60.0
    if lag > max_lag:
        return False, "data lag: the last completed bar ended %.0f min ago (limit %d)" % (lag, max_lag), age
    dt = parse_local(entry_doc.get("data_time"))
    if dt is not None and dt <= now_local:
        dlag = (now_local - dt).total_seconds() / 60.0
        if dlag > max_lag:
            return False, "data lag: Yahoo's last update was %.0f min ago (limit %d)" % (dlag, max_lag), age
    return True, "fresh", age


def hit_age(hit, now_local):
    t = parse_hhmm_ampm(hit.get("at"))
    if t is None:
        return None
    return (now_local - datetime.datetime.combine(now_local.date(), t)).total_seconds() / 60.0


# ---------------------------------------------------------------- news

def company_word(name):
    for w in re.split(r"[\s,.()]+", name or ""):
        if len(w) >= 4 and w.lower() not in GENERIC_WORDS and w[0].isupper():
            return w
    return None


def mentions(title, symbol, word):
    sym = re.escape(symbol.replace("-", "."))
    pats = [r"\(%s\)" % sym, r"(?:NYSE|NASDAQ|NasdaqGS|NasdaqGM|NasdaqCM)\s*:\s*%s\b" % sym]
    if len(symbol) >= 3:
        pats.append(r"\b%s\b" % sym)
    if word:
        pats.append(r"\b%s\b" % re.escape(word))
    return any(re.search(p, title or "") for p in pats)


def news_flags(doc, symbol, name, move_limit):
    """-> list of reasons this candidate carries event risk (empty = none found)."""
    out = []
    word = company_word(name)
    for m in doc.get("moves") or []:
        if m.get("ticker") in (symbol, symbol.replace(".", "-")) and m.get("pct") is not None:
            if abs(m["pct"]) >= move_limit:
                out.append("big move today: %+.2f%% vs yesterday's close (event-driven?)" % m["pct"])
    for it in doc.get("items") or []:
        tags = set(it.get("tags") or [])
        hot = tags & EVENT_TAGS
        if hot and mentions(it.get("title", ""), symbol, word):
            out.append("headline [%s]: %s" % (", ".join(sorted(hot)), it.get("title", "")[:140]))
    return out


# ---------------------------------------------------------------- time machine

def expectancy(tm, min_history):
    """-> (ok, reason). Negative or unproven expectancy is a WAIT, never a signal."""
    mc = tm.get("mc") or {}
    hist = tm.get("history") or {}
    mc_r, h_r, n = mc.get("avg_R"), hist.get("avg_R"), hist.get("triggered") or 0
    if mc_r is None:
        return False, "time machine gave no average R"
    if mc_r <= 0:
        return False, "negative expectancy: replays average R %.3f" % mc_r
    if n < min_history:
        return False, "too little history: the rule fired %d times in %s sessions (need %d)" % (
            n, hist.get("sessions"), min_history)
    if h_r is None or h_r <= 0:
        return False, "negative expectancy: history average R %s over %d trades" % (h_r, n)
    return True, "positive in replays (%.3f R) and history (%.3f R, %d trades)" % (mc_r, h_r, n)


# ---------------------------------------------------------------- state

def load_state(path):
    try:
        with open(path) as fh:
            st = json.load(fh)
        if isinstance(st, dict) and isinstance(st.get("days"), dict):
            return st
    except (OSError, ValueError):
        pass
    return {"version": 1, "days": {}}


def day_state(state, day):
    d = state["days"].setdefault(day, {})
    d.setdefault("seen", {})
    d.setdefault("sent_orders", [])
    d.setdefault("alerts", {})
    return d


def save_state(state, path, today):
    cutoff = (datetime.date.fromisoformat(today) - datetime.timedelta(days=KEEP_DAYS)).isoformat()
    state["days"] = {k: v for k, v in state["days"].items() if k >= cutoff}
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)
    os.replace(tmp, path)


# ---------------------------------------------------------------- telegram

def send_telegram(text, token, chat_id, timeout=20):
    """-> (ok, error). The token is never printed."""
    url = "https://api.telegram.org/bot%s/sendMessage" % token
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text,
                                   "disable_web_page_preview": "true"}).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=timeout) as r:
            body = json.load(r)
        return (True, None) if body.get("ok") else (False, str(body.get("description")))
    except urllib.error.HTTPError as e:
        return False, "HTTP %s" % e.code
    except (urllib.error.URLError, OSError, ValueError) as e:
        return False, type(e).__name__


def money(x):
    return "${:,.2f}".format(x)


def format_signal(s):
    p, sz, tm = s["plan"], s["sizing"], s["tm"]
    mc, hist = tm.get("mc") or {}, tm.get("history") or {}
    return "\n".join([
        "🟢 SANAL ALIM SİNYALİ — %s" % s["symbol"],
        "Kurulum: 15 dk açılış aralığı kırılımı (5 dk çubuklar, Yahoo)",
        "ALIM KOŞULU: %s çubuğu açılış aralığı yükseği %s üzerinde kapandı" % (
            s["entry"]["trigger"]["bar_start"], money(s["entry"]["level"])),
        "Tetik: %s ET %s · veri: son tamamlanan çubuk %s ET" % (
            p["entry_time"], p["date"], s["entry"]["last"]["bar_start"]),
        "GİRİŞ: %s" % money(p["entry"]),
        "STOP-LOSS: %s (açılış aralığı ortası)" % money(p["stop"]),
        "KÂR HEDEFİ: %s (2R)" % money(p["target"]),
        "RİSK/ÖDÜL: %.2f" % p["reward_risk"],
        "Adet: %s (≈%s) · maks. kayıp %s" % (sz["qty"], money(sz["cost"]), money(sz["max_loss"])),
        "GEÇERLİLİK: en geç %s ET'ye kadar giriş; %s ET'de pozisyonu kapat" % (s["valid_until"], p["flat_by"]),
        "TimeMachine (seed 42): hedef önce %.1f%% · stop önce %.1f%% · ort. R %.3f" % (
            100 * mc.get("p_target", 0), 100 * mc.get("p_stop", 0), mc.get("avg_R", 0)),
        "Geçmiş: %s sinyal, ort. R %s" % (hist.get("triggered"), hist.get("avg_R")),
        "trader-risk: PASS",
        "Sanal plan — gerçek emir gönderilmedi. Yatırım tavsiyesi değildir.",
    ])


# ---------------------------------------------------------------- runner

class Runner:
    """Runs one script in the work folder. In replay mode (now_utc given) every script gets the
    pinned clock: news.py reads --now as UTC, the others as New York time."""

    def __init__(self, work, now_utc, log):
        self.work, self.log = work, log
        self.now_ny = ny_now(now_utc).strftime("%Y-%m-%d %H:%M") if now_utc else None
        self.now_z = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if now_utc else None

    def run(self, key, args, timeout=600):
        cmd = [PY, SCRIPTS[key]] + list(args)
        if self.now_ny and not (key == "scan" and args and args[0] == "universe"):
            cmd += ["--now", self.now_z if key == "news" else self.now_ny]
        shown = " ".join(os.path.relpath(c, ROOT) if c.startswith(ROOT) else c for c in cmd[1:])
        try:
            p = subprocess.run(cmd, cwd=self.work, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            self.log.append((shown, "timeout"))
            return None, "", "timeout"
        self.log.append((shown, p.returncode))
        return p.returncode, p.stdout, p.stderr

    def run_json(self, key, args, timeout=600):
        code, out, err = self.run(key, args, timeout)
        try:
            return code, json.loads(out), err
        except ValueError:
            return code, None, (err or out or "").strip()[-300:]


def universe_names(path):
    names = {}
    try:
        with open(path) as fh:
            for line in fh:
                if "|" in line:
                    s, n = line.rstrip("\n").split("|", 1)
                    names[s] = n
    except OSError:
        pass
    return names


def evaluate(r, sym, name, now_local, a, sent_orders):
    """One candidate through entry -> freshness -> news -> exit -> size -> risk -> time machine.
    -> (status, reason, signal). status: signal | rejected | error (error = try again next run)."""
    d = os.path.join("trader", "c", sym)
    os.makedirs(os.path.join(r.work, d), exist_ok=True)
    code, ent, err = r.run_json("entry", ["check", sym, "--json", "--write", d + "/entry.json"])
    if ent is None:
        return "error", "entry.py failed: %s" % err, None
    ok, why, _ = freshness(ent, now_local, a.max_signal_age, a.max_data_lag)
    if not ok:
        return "rejected", why, None
    code, news, err = r.run_json("news", ["--tickers", sym, "--hours", "24", "--no-sec", "--dry-run",
                                          "--json", "--state", d + "/news-state.json"])
    if news is None:
        return "error", "news.py failed (news unverified): %s" % err, None
    flags = news_flags(news, sym, name, a.move_limit)
    if flags:
        return "rejected", "event risk: " + flags[0], None
    code, plan, err = r.run_json("exit", ["plan", "--from", d + "/entry.json", "--write", d + "/plan.json", "--json"])
    if plan is None:
        return "rejected", "exit.py gave no plan: %s" % err, None
    code, size, err = r.run_json("size", ["--plan", d + "/plan.json", "--write", d + "/orders.json", "--json"])
    if size is None or not size.get("orders"):
        why = (size or {}).get("sizing", {}).get("refusal") or err or "no order"
        return "rejected", "size.py: %s" % why, None
    new = size["orders"][0]
    with open(os.path.join(r.work, d, "risk-orders.json"), "w") as fh:
        json.dump({"created_by": "trader-signals", "paper": True, "orders": list(sent_orders) + [new]}, fh)
    code, risk, err = r.run_json("risk", ["--orders", d + "/risk-orders.json", "--no-write", "--json"])
    if risk is None or not risk.get("results"):
        return "error", "check.py failed: %s" % err, None
    last = risk["results"][-1]
    if last.get("verdict") != "PASS":
        return "rejected", "trader-risk BLOCK: %s" % "; ".join(last.get("reasons") or ["blocked"]), None
    code, tm, err = r.run_json("tm", ["run", "--plan", d + "/plan.json", "--seed", "42",
                                      "--until", plan.get("flat_by", "15:55"), "--json"], timeout=900)
    if tm is None:
        return "error", "timemachine.py failed: %s" % err, None
    ok, why = expectancy(tm, a.min_history)
    if not ok:
        return "rejected", "BEKLE — " + why, {"tm": tm, "plan": plan}
    trig = parse_hhmm_ampm(ent["trigger"]["close_time"])
    valid = (datetime.datetime.combine(now_local.date(), trig) + datetime.timedelta(minutes=a.max_signal_age)).time()
    cutoff = parse_hhmm_ampm(ent.get("no_entries_after")) or WINDOW_END
    valid = min(valid, cutoff)
    return "signal", why, {"symbol": sym, "entry": ent, "plan": plan, "sizing": size["sizing"],
                            "order": new, "risk": last, "tm": tm, "valid_until": valid.strftime("%H:%M")}


def report_md(header, verdict, signals, rejected, log, notes):
    out = ["## " + header, "", "**%s**" % verdict, ""]
    if signals:
        out += ["| Hisse | Giriş | Stop-loss | Hedef | R/Ö | Adet | Maks. kayıp | Geçerlilik | TM ort. R | Geçmiş ort. R |",
                "|---|---|---|---|---|---|---|---|---|---|"]
        for s in signals:
            p, z, tm = s["plan"], s["sizing"], s["tm"]
            out.append("| %s | %s @ %s | %s | %s | %.2f | %s | %s | %s'e kadar, kapat %s | %.3f | %s |" % (
                s["symbol"], money(p["entry"]), p["entry_time"], money(p["stop"]), money(p["target"]),
                p["reward_risk"], z["qty"], money(z["max_loss"]), s["valid_until"], p["flat_by"],
                tm["mc"]["avg_R"], tm["history"]["avg_R"]))
        out.append("")
    if rejected:
        out += ["Elenenler:", ""] + ["- %s: %s" % (s, why) for s, why in rejected] + [""]
    if notes:
        out += ["Notlar:", ""] + ["- " + n for n in notes] + [""]
    if log:
        out += ["Çalışan betikler:", "", "```"] + ["exit=%s  %s" % (c, cmd) for cmd, c in log] + ["```", ""]
    out.append("Sanal plan — gerçek emir gönderilmedi, broker bağlantısı yok. Yatırım tavsiyesi değildir.")
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--workdir", default="work", help="scratch folder (trader/ goes inside)")
    ap.add_argument("--state", help="dedup state file (default <workdir>/trader/state/signals.json)")
    ap.add_argument("--now", help='replay: pretend it is this New York time, "2026-11-02 10:20" (never sends)')
    ap.add_argument("--dry-run", action="store_true", help="do not send to Telegram")
    ap.add_argument("--max-candidates", type=int, default=5)
    ap.add_argument("--max-signal-age", type=int, default=20, help="minutes since the trigger bar closed")
    ap.add_argument("--max-data-lag", type=int, default=10, help="minutes since the last completed bar")
    ap.add_argument("--move-limit", type=float, default=8.0, help="a day move this big (%%) is event risk")
    ap.add_argument("--min-history", type=int, default=10, help="past triggers the time machine needs")
    ap.add_argument("--telegram-test", action="store_true", help="send one test message and stop")
    a = ap.parse_args(argv)

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    can_send = bool(token and chat) and not a.dry_run and not a.now

    if a.telegram_test:
        if not (token and chat):
            print("Telegram is not configured: set the TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID secrets.")
            return 1
        ok, err = send_telegram("✅ Trader sinyal botu bağlantı testi. Bu bir sinyal değildir.", token, chat)
        print("Telegram test: %s" % ("sent" if ok else "FAILED (%s)" % err))
        return 0 if ok else 1

    work = os.path.abspath(a.workdir)
    os.makedirs(os.path.join(work, "trader"), exist_ok=True)
    state_path = os.path.abspath(a.state or os.path.join(work, "trader", "state", "signals.json"))
    try:
        now_utc = SCAN.parse_now(a.now)
    except SCAN.ScanError as e:
        print(e, file=sys.stderr)
        return 2
    now_local = ny_now(now_utc)
    today = now_local.date().isoformat()
    state = load_state(state_path)
    ds = day_state(state, today)
    header = "Trader taraması — NY %s ET%s" % (now_local.strftime("%Y-%m-%d %H:%M"),
                                               " (REPLAY, gönderim yok)" if a.now else "")
    log, notes, rejected, signals = [], [], [], []

    def alert_once(key, text):
        if can_send and not ds["alerts"].get(key):
            ok, err = send_telegram(text, token, chat)
            if ok:
                ds["alerts"][key] = now_local.isoformat()
            else:
                notes.append("Telegram alert failed: %s" % err)

    def finish(verdict):
        md = report_md(header, verdict, signals, rejected, log, notes)
        print(md)
        summ = os.environ.get("GITHUB_STEP_SUMMARY")
        if summ:
            with open(summ, "a") as fh:
                fh.write(md + "\n")
        with open(os.path.join(work, "trader", "report.md"), "w") as fh:
            fh.write(md + "\n")
        save_state(state, state_path, today)
        return 0

    ok, why, _ = gate(now_utc)
    if not ok:
        if why == "calendar_unknown":
            notes.append("NYSE calendar in the scripts covers %s only: no signals until it is updated."
                         % ", ".join(map(str, SCAN.CALENDAR_YEARS)))
            alert_once("calendar", "⚠️ Trader botu: NYSE takvimi %d yılını kapsamıyor, sinyal üretilmiyor. "
                                   "Betiklerdeki NYSE_HOLIDAYS tablosu güncellenmeli." % now_local.year)
        return finish("Pencere dışı / piyasa kapalı — çalışma yok (%s)" % why)

    cfg_dst = os.path.join(work, "trader", "config.json")
    with open(os.path.join(HERE, "config.json")) as src, open(cfg_dst, "w") as dst:
        dst.write(src.read())

    r = Runner(work, now_utc if a.now else None, log)
    code, _, err = r.run("scan", ["universe", "--out", "trader/universe.txt"], timeout=300)
    if code != 0 and not os.path.exists(os.path.join(work, "trader", "universe.txt")):
        notes.append("universe download failed: %s" % (err or "").strip()[-200:])
        alert_once("data_error", "⚠️ Trader botu: sembol listesi indirilemedi; bugün sinyal yok olabilir.")
        return finish("BEKLE — veri alınamadı")
    names = universe_names(os.path.join(work, "trader", "universe.txt"))

    code, orb, err = r.run_json("scan", ["orb", "--json", "--quiet", "--out", "trader/orb.json"], timeout=900)
    if orb is None:
        notes.append("scan.py orb failed: %s" % err)
        alert_once("data_error", "⚠️ Trader botu: Yahoo verisi alınamadı (scan.py orb). Sinyal gönderilmedi.")
        return finish("BEKLE — veri alınamadı")
    c = orb.get("counts") or {}
    notes.append("scan.py orb: as of %s, %s of %s charts scanned, %s breakouts (%s unconfirmed)." % (
        orb.get("as_of"), c.get("scanned"), c.get("requested"), c.get("hits"), orb.get("unconfirmed")))

    fresh_hits = []
    for h in orb.get("hits") or []:
        sym = h["symbol"]
        if sym in ds["seen"]:
            continue
        age = hit_age(h, now_local)
        if age is None or age > a.max_signal_age:
            continue  # an older breakout: was (or should have been) handled by an earlier run
        fresh_hits.append(h)
    notes.append("%d new breakouts from the last %d minutes; checking up to %d." % (
        len(fresh_hits), a.max_signal_age, a.max_candidates))

    for h in fresh_hits[: a.max_candidates]:
        sym = h["symbol"]
        status, why, sig = evaluate(r, sym, names.get(sym, ""), now_local, a, ds["sent_orders"])
        if status == "error":
            rejected.append((sym, "hata (sonraki çalışmada tekrar denenecek): " + why))
            continue
        if status == "rejected":
            rejected.append((sym, why))
            ds["seen"][sym] = {"status": "rejected", "reason": why, "at": now_local.isoformat()}
            continue
        text = format_signal(sig)
        if can_send:
            ok, err = send_telegram(text, token, chat)
            if not ok:
                rejected.append((sym, "Telegram gönderilemedi (%s); sonraki çalışmada tazeyse tekrar denenir" % err))
                continue
        signals.append(sig)
        ds["seen"][sym] = {"status": "sent" if can_send else "found", "at": now_local.isoformat(),
                           "entry": sig["plan"]["entry"], "stop": sig["plan"]["stop"],
                           "target": sig["plan"]["target"]}
        ds["sent_orders"].append(sig["order"])

    if not can_send:
        notes.append("Telegram: %s." % ("replay mode, nothing sent" if a.now else
                                        "dry run, nothing sent" if a.dry_run else
                                        "not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID secrets missing)"))
    verdict = ("%d yeni sanal alım sinyali" % len(signals)) if signals else "BEKLE — geçerli sinyal yok"
    return finish(verdict)


if __name__ == "__main__":
    sys.exit(main())
