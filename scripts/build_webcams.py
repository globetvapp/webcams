#!/usr/bin/env python3
"""
build_webcams.py  (v2 - fix validare YouTube)
---------------------------------------------
Trage streams.geojson din willytop8/Live-Environment-Streams, pastreaza DOAR
ce se poate pune in embed (url_type = hls | youtube), sanitizeaza fiecare
stream si scrie webcams.json gata de folosit pe site.

FIX v2: pe runnerele GitHub (IP-uri de datacenter) YouTube servea pagina de
bot-check ("Sign in to confirm you're not a bot" -> status LOGIN_REQUIRED /
UNPLAYABLE), iar v1 o interpreta gresit ca "video mort" si scotea toate
streamurile YouTube. Acum:
  * validarea YouTube foloseste endpoint-ul oEmbed (mult mai rar bot-walled);
  * bot-wall-urile (LOGIN_REQUIRED / UNPLAYABLE / 429 / lipsa) NU mai scot
    niciodata un stream -> raman ca "unknown"/"ok";
  * un YouTube e scos DOAR daca oEmbed spune clar sters (404) sau privat (401),
    ori daca o pagina watch curata (fara bot-wall) zice playableInEmbed:false.

Sanitizare pe "strikes" (webcams_state.json): un stream e scos doar dupa
STRIKE_LIMIT esecuri consecutive. "ok" reseteaza contorul -> streamurile scoase
din greseala in v1 revin automat la prima rulare buna.
"""

import os
import re
import json
import time
import random
import hashlib
import argparse
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------- config
SOURCE_URL = os.environ.get(
    "SOURCE_URL",
    "https://raw.githubusercontent.com/willytop8/Live-Environment-Streams/main/streams.geojson",
)
OUTPUT     = os.environ.get("OUTPUT", "webcams.json")
STATE_FILE = os.environ.get("STATE_FILE", "webcams_state.json")

STRIKE_LIMIT  = int(os.environ.get("STRIKE_LIMIT", "3"))
BROKEN_WEIGHT = int(os.environ.get("BROKEN_WEIGHT", "3"))
DROP_HTTP_HLS = os.environ.get("DROP_HTTP_HLS", "true").lower() == "true"
TIMEOUT       = float(os.environ.get("TIMEOUT", "8"))
HLS_WORKERS   = int(os.environ.get("HLS_WORKERS", "32"))
YT_WORKERS    = int(os.environ.get("YT_WORKERS", "6"))   # blandut cu YouTube

EMBEDDABLE_TYPES = {"hls", "youtube"}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
YT_HEADERS  = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
YT_COOKIES  = {"CONSENT": "YES+1", "SOCS": "CAI"}
HLS_HEADERS = {"User-Agent": UA}

VIDEO_ID_RE   = re.compile(r'(?:v=|/embed/|youtu\.be/|/live/)([\w-]{11})')
ANY_VID_RE    = re.compile(r'"videoId":"([\w-]{11})"')
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
            "_url": url,
        }
        if t == "youtube":
            m = VIDEO_ID_RE.search(url)
            rec["video_id"] = m.group(1) if m else None
            if not rec["video_id"]:
                rec["_needs_resolve"] = True
                stats["yt_unresolved"] += 1
        out.append(rec)
    return out, stats


# ---------------------------------------------------------------- validators
def resolve_channel_live(url, session):
    try:
        r = session.get(url, timeout=TIMEOUT, headers=YT_HEADERS, cookies=YT_COOKIES)
        if r.status_code == 200:
            m = ANY_VID_RE.search(r.text)
            return m.group(1) if m else None
    except Exception:
        pass
    return None


def check_youtube(video_id, session):
    """
    oEmbed = sursa principala (rezistenta la bot-check). Bot-wall-urile de pe
    pagina watch NU scot niciodata streamul.
    """
    oembed = ("https://www.youtube.com/oembed?url="
              f"https://www.youtube.com/watch?v={video_id}&format=json")
    try:
        oe = session.get(oembed, timeout=TIMEOUT, headers=YT_HEADERS)
    except Exception:
        return "unknown", {"reason": "oembed_error"}

    if oe.status_code in (401, 403):
        return "broken", {"reason": "private_or_embed_off"}
    if oe.status_code == 404:
        return "broken", {"reason": "deleted"}
    if oe.status_code != 200:
        return "unknown", {"reason": f"oembed_{oe.status_code}"}

    # exista si e public. rafinam DOAR pe o pagina watch curata (fara bot-wall).
    meta = {}
    try:
        w = session.get(f"https://www.youtube.com/watch?v={video_id}",
                        timeout=TIMEOUT, headers=YT_HEADERS, cookies=YT_COOKIES)
        h = w.text
        clean = ("playabilityStatus" in h
                 and "consent.youtube.com" not in h
                 and "/recaptcha/" not in h)
        if clean:
            m = PLAYSTATUS_RE.search(h)
            status = m.group(1) if m else None
            if status == "OK":
                if '"playableInEmbed":false' in h:
                    return "broken", {"reason": "embed_disabled"}
                meta["is_live"] = ('"isLive":true' in h or '"isLiveNow":true' in h)
            # LOGIN_REQUIRED / UNPLAYABLE / lipsa -> bot-wall -> ignoram, pastram oEmbed OK
    except Exception:
        pass
    return "ok", meta


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
        if "#EXTM3U" in r.text[:4096]:
            return "ok", {}
        return "unknown", {"reason": "not_a_playlist"}
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
            return rec, "unknown", {"reason": "no_video_id"}   # nu scoatem pe un fail tranzitoriu
        verdict, meta = check_youtube(vid, session)
        time.sleep(random.uniform(0.2, 0.6))                   # jitter -> mai putine bot-check
        return rec, verdict, meta
    return (rec, *check_hls(rec["_url"], session))


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--source")
    args = ap.parse_args()

    if args.source:
        geojson = load_json(args.source, {"features": []})
    else:
        print(f"-> descarc: {SOURCE_URL}")
        geojson = fetch_source()

    candidates, fstats = build_candidates(geojson)
    print(f"-> candidati embeddable: {len(candidates)} "
          f"(http-hls scoase: {fstats['http_hls_dropped']}, "
          f"yt de rezolvat: {fstats['yt_unresolved']})")

    state = load_json(STATE_FILE, {})
    ok = broken = unknown = 0

    if args.offline:
        for rec in candidates:
            st = state.get(rec["id"], {"fails": 0, "last_ok": None})
            st.update(fails=0, last_ok=now_iso(), last_check=now_iso(), type=rec["type"])
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
        print(f"-> probez {len(yt)} YouTube via oEmbed (workers={YT_WORKERS}) ...")
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

    live_ids = {c["id"] for c in candidates}
    for dead in [k for k in state if k not in live_ids]:
        del state[dead]

    by_id = {c["id"]: c for c in candidates}
    clean = []
    for cid, st in state.items():
        if st["fails"] >= STRIKE_LIMIT:
            continue
        rec = by_id.get(cid)
        if not rec:
            continue
        if rec["type"] == "youtube" and not rec.get("video_id"):
            continue  # fara video_id nu avem ce embeda
        out = {k: v for k, v in rec.items() if not k.startswith("_")}
        if rec["type"] == "youtube":
            out["embed"] = (f"https://www.youtube.com/embed/"
                            f"{rec['video_id']}?autoplay=1&mute=1&playsinline=1")
        else:
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
        f"Verdicte: ok={ok}, broken={broken}, unknown={unknown} | "
        f"prag strikes={STRIKE_LIMIT}, state={len(state)}"
    )
    print("\n" + summary)
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as fh:
            fh.write("### Webcams refresh\n```\n" + summary + "\n```\n")


if __name__ == "__main__":
    main()
