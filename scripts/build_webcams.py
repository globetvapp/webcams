#!/usr/bin/env python3
"""
build_webcams.py
----------------
Trage streams.geojson din willytop8/Live-Environment-Streams, pastreaza DOAR
ce se poate pune in embed (url_type = hls | youtube), sanitizeaza fiecare
stream si scrie webcams.json gata de folosit pe site.

Sanitizarea NU e o singura verificare: foloseste un sistem de "strikes"
(webcams_state.json). Un stream e scos de pe site DOAR dupa STRIKE_LIMIT
verificari nereusite la rand -> un hiccup temporar de retea sau un bot-check
YouTube nu-ti sterge baza de date, dar streamurile chiar moarte dispar in
cateva rulari.

Rezultat (verdicte per stream):
  ok      -> merge, fails=0
  broken  -> dovada clara ca e mort (404/410, video sters/privat, neembeddable)
  unknown -> nu s-a putut determina (timeout, 403/429, bot-wall) -> nu penalizam dur

Config prin variabile de mediu (vezi workflow-ul):
  SOURCE_URL, STRIKE_LIMIT, BROKEN_WEIGHT, REQUIRE_LIVE, TIMEOUT,
  HLS_WORKERS, YT_WORKERS, DROP_HTTP_HLS
Ruleaza cu --offline ca sa sari peste probare (doar filtrare + extragere).
"""

import os
import re
import sys
import json
import time
import random
import hashlib
import argparse
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------- config
SOURCE_URL   = os.environ.get(
    "SOURCE_URL",
    "https://raw.githubusercontent.com/willytop8/Live-Environment-Streams/main/streams.geojson",
)
OUTPUT       = os.environ.get("OUTPUT", "webcams.json")
STATE_FILE   = os.environ.get("STATE_FILE", "webcams_state.json")

STRIKE_LIMIT   = int(os.environ.get("STRIKE_LIMIT", "3"))    # cate esecuri la rand pana scoatem
BROKEN_WEIGHT  = int(os.environ.get("BROKEN_WEIGHT", "3"))   # cat "cantareste" un broken clar (=limit -> scoatere instant)
REQUIRE_LIVE   = os.environ.get("REQUIRE_LIVE", "true").lower() == "true"
DROP_HTTP_HLS  = os.environ.get("DROP_HTTP_HLS", "true").lower() == "true"
TIMEOUT        = float(os.environ.get("TIMEOUT", "8"))
HLS_WORKERS    = int(os.environ.get("HLS_WORKERS", "32"))
YT_WORKERS     = int(os.environ.get("YT_WORKERS", "8"))      # blandut cu YouTube ca sa nu declansam bot-check

EMBEDDABLE_TYPES = {"hls", "youtube"}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
YT_HEADERS  = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
YT_COOKIES  = {"CONSENT": "YES+1", "SOCS": "CAI"}
HLS_HEADERS = {"User-Agent": UA}

VIDEO_ID_RE = re.compile(r'(?:v=|/embed/|youtu\.be/|/live/)([\w-]{11})')
ANY_VID_RE  = re.compile(r'"videoId":"([\w-]{11})"')
PLAYSTATUS_RE = re.compile(r'"playabilityStatus":\{"status":"([A-Z_]+)"')

now_iso = lambda: dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- helpers
def stable_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def fetch_source():
    import requests
    r = requests.get(SOURCE_URL, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------- filter + normalize
def build_candidates(geojson):
    """Filtreaza la embeddable, extrage videoId, construieste recordurile."""
    out, seen, stats = [], set(), {"http_hls_dropped": 0, "yt_unresolved": 0}
    for feat in geojson.get("features", []):
        p = feat.get("properties", {})
        t = p.get("url_type")
        if t not in EMBEDDABLE_TYPES:
            continue
        url = (p.get("url") or "").strip()
        if not url:
            continue
        if t == "hls" and DROP_HTTP_HLS and url.lower().startswith("http://"):
            stats["http_hls_dropped"] += 1
            continue

        sid = stable_id(url)
        if sid in seen:
            continue
        seen.add(sid)

        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None]
        rec = {
            "id": sid,
            "name": p.get("name") or p.get("display_name") or "Unknown",
            "country": p.get("country_code"),
            "lat": coords[1] if len(coords) > 1 else None,
            "lon": coords[0] if len(coords) > 0 else None,
            "environment": p.get("environment"),
            "scene": p.get("scene_type"),
            "type": t,
            "_url": url,          # camp intern, scos la final
        }
        if t == "youtube":
            m = VIDEO_ID_RE.search(url)
            rec["video_id"] = m.group(1) if m else None
            if not rec["video_id"]:
                # e o adresa /live sau /channel -> se rezolva la probare
                rec["_needs_resolve"] = True
                stats["yt_unresolved"] += 1
        out.append(rec)
    return out, stats


# ---------------------------------------------------------------- validators
def resolve_channel_live(url, session):
    """Pentru URL-uri /live sau /channel: scoate videoId-ul curent din HTML."""
    try:
        r = session.get(url, timeout=TIMEOUT, headers=YT_HEADERS, cookies=YT_COOKIES)
        if r.status_code == 200:
            m = ANY_VID_RE.search(r.text)
            return m.group(1) if m else None
    except Exception:
        pass
    return None


def check_youtube(video_id, session):
    """(verdict, meta). verdict in {ok, broken, unknown}."""
    try:
        r = session.get(f"https://www.youtube.com/watch?v={video_id}",
                        timeout=TIMEOUT, headers=YT_HEADERS, cookies=YT_COOKIES)
    except Exception:
        return "unknown", {"reason": "request_error"}
    if r.status_code != 200:
        return "unknown", {"reason": f"http_{r.status_code}"}

    h = r.text
    # bot-check / consent wall -> nu putem sti, nu penalizam
    if "playabilityStatus" not in h or "consent.youtube.com" in h or "/recaptcha/" in h:
        return "unknown", {"reason": "bot_wall"}

    m = PLAYSTATUS_RE.search(h)
    status = m.group(1) if m else None
    embeddable = '"playableInEmbed":true' in h
    is_live = '"isLive":true' in h or '"isLiveNow":true' in h

    if status in ("ERROR", "LOGIN_REQUIRED", "UNPLAYABLE"):
        return "broken", {"reason": f"status_{status}"}
    if not embeddable:
        return "broken", {"reason": "not_embeddable"}
    if status == "LIVE_STREAM_OFFLINE":
        # e videoul corect dar offline acum -> tranzitoriu, lasam strikes sa decida
        return "unknown", {"reason": "offline_now", "is_live": False}
    if REQUIRE_LIVE and not is_live:
        return "unknown", {"reason": "not_live_now", "is_live": False}
    return "ok", {"is_live": is_live}


def check_hls(url, session):
    import requests
    try:
        r = session.get(url, timeout=TIMEOUT, headers=HLS_HEADERS)
    except requests.exceptions.SSLError:
        return "broken", {"reason": "ssl_error"}
    except requests.exceptions.ConnectionError:
        return "broken", {"reason": "conn_refused"}
    except Exception:
        return "unknown", {"reason": "request_error"}

    code = r.status_code
    if code in (404, 410):
        return "broken", {"reason": f"http_{code}"}
    if code == 200:
        head = r.text[:4096]
        if "#EXTM3U" in head:
            return "ok", {}
        return "unknown", {"reason": "not_a_playlist"}
    if code in (401, 403, 429) or 500 <= code < 600:
        return "unknown", {"reason": f"http_{code}"}      # posibil tranzitoriu
    return "unknown", {"reason": f"http_{code}"}


def probe(rec):
    import requests
    session = requests.Session()
    if rec["type"] == "youtube":
        vid = rec.get("video_id")
        if not vid and rec.get("_needs_resolve"):
            vid = resolve_channel_live(rec["_url"], session)
            rec["video_id"] = vid
        if not vid:
            return rec, "broken", {"reason": "no_video_id"}
        verdict, meta = check_youtube(vid, session)
        time.sleep(random.uniform(0.15, 0.5))   # jitter -> mai putine bot-check
        return rec, verdict, meta
    else:
        return (rec, *check_hls(rec["_url"], session))


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true",
                    help="doar filtrare+extragere, fara probare live")
    ap.add_argument("--source", help="cale locala catre streams.geojson (test)")
    args = ap.parse_args()

    if args.source:
        geojson = load_json(args.source, {"features": []})
    else:
        print(f"-> descarc sursa: {SOURCE_URL}")
        geojson = fetch_source()

    candidates, fstats = build_candidates(geojson)
    print(f"-> candidati embeddable: {len(candidates)} "
          f"(http-hls scoase: {fstats['http_hls_dropped']}, "
          f"yt de rezolvat: {fstats['yt_unresolved']})")

    state = load_json(STATE_FILE, {})
    ok = broken = unknown = 0

    if args.offline:
        # toate trec ca "ok" ca sa putem verifica pipeline-ul fara retea
        for rec in candidates:
            st = state.get(rec["id"], {"fails": 0, "last_ok": None})
            st.update(fails=0, last_ok=now_iso(), last_check=now_iso(),
                      type=rec["type"])
            state[rec["id"]] = st
            ok += 1
    else:
        hls = [c for c in candidates if c["type"] == "hls"]
        yt  = [c for c in candidates if c["type"] == "youtube"]

        results = []
        print(f"-> probez {len(hls)} HLS (workers={HLS_WORKERS}) ...")
        with ThreadPoolExecutor(max_workers=HLS_WORKERS) as ex:
            for fut in as_completed(ex.submit(probe, c) for c in hls):
                results.append(fut.result())
        print(f"-> probez {len(yt)} YouTube (workers={YT_WORKERS}, jitter) ...")
        with ThreadPoolExecutor(max_workers=YT_WORKERS) as ex:
            for fut in as_completed(ex.submit(probe, c) for c in yt):
                results.append(fut.result())

        for rec, verdict, meta in results:
            st = state.get(rec["id"], {"fails": 0, "last_ok": None})
            if verdict == "ok":
                st["fails"] = 0
                st["last_ok"] = now_iso()
                ok += 1
            elif verdict == "broken":
                st["fails"] = st.get("fails", 0) + BROKEN_WEIGHT
                broken += 1
            else:
                st["fails"] = st.get("fails", 0) + 1
                unknown += 1
            st["last_check"] = now_iso()
            st["type"] = rec["type"]
            if meta.get("reason"):
                st["last_reason"] = meta["reason"]
            if "is_live" in meta:
                rec["is_live"] = meta["is_live"]
            state[rec["id"]] = st

    # curata din state ce nu mai exista in sursa (evita cresterea la infinit)
    live_ids = {c["id"] for c in candidates}
    for dead in [k for k in state if k not in live_ids]:
        del state[dead]

    # construieste lista curata: doar sub pragul de strikes
    by_id = {c["id"]: c for c in candidates}
    clean = []
    for cid, st in state.items():
        if st["fails"] >= STRIKE_LIMIT:
            continue
        rec = by_id.get(cid)
        if not rec:
            continue
        out = {k: v for k, v in rec.items() if not k.startswith("_")}
        if rec["type"] == "youtube" and rec.get("video_id"):
            out["embed"] = (f"https://www.youtube.com/embed/"
                            f"{rec['video_id']}?autoplay=1&mute=1&playsinline=1")
        elif rec["type"] == "hls":
            out["src"] = rec["_url"]
        out["verified"] = st.get("fails", 0) == 0
        out["last_ok"] = st.get("last_ok")
        clean.append(out)

    clean.sort(key=lambda r: (r.get("country") or "ZZ", r.get("name") or ""))

    payload = {
        "generated_at": now_iso(),
        "source": SOURCE_URL,
        "count": len(clean),
        "by_type": {
            "youtube": sum(1 for r in clean if r["type"] == "youtube"),
            "hls":     sum(1 for r in clean if r["type"] == "hls"),
        },
        "webcams": clean,
    }
    with open(OUTPUT, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    with open(STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=1)

    summary = (
        f"Webcams live: {len(clean)} "
        f"(YouTube {payload['by_type']['youtube']}, HLS {payload['by_type']['hls']})\n"
        f"Verdicte aceasta rulare: ok={ok}, broken={broken}, unknown={unknown}\n"
        f"Prag strikes={STRIKE_LIMIT}, intrari in state={len(state)}"
    )
    print("\n" + summary)

    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as fh:
            fh.write("### Webcams refresh\n```\n" + summary + "\n```\n")


if __name__ == "__main__":
    main()