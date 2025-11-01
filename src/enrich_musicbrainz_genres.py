# src/fill_missing_genres_musicbrainz.py
import pandas as pd
import requests
import math
from pathlib import Path
import time, random, requests

# ---- paths (adjust if needed) ----
INPUT_CSV  = Path("data/enriched/artist_genres_raw4 copy.csv")
OUTPUT_CSV = Path("data/enriched/artist_genres_raw4_mb.csv")
SAVE_EVERY = 100

MB_SESSION = requests.Session()
UA = {"User-Agent": "rock-evolution/1.0 (contact: your-email@example.com)"}

# polite pacing + retry settings
MB_MIN_INTERVAL   = 1.1     # ≥ 1 req/sec per MB guidelines
MB_TIMEOUT        = 45      # seconds (increase from 20)
MB_MAX_RETRIES    = 4       # total attempts
MB_BACKOFF_FACTOR = 1.6     # 1.6^n + jitter
_mb_last_ts       = 0.0

# ---------- helpers ----------
NULL_STRINGS = {"", "nan", "none", "null"}

def is_blank(v) -> bool:
    """True if v is NaN/None/empty string or a null-like string."""
    if v is None:
        return True
    # pandas NaN
    if isinstance(v, float) and math.isnan(v):
        return True
    s = str(v).strip().lower()
    return s in NULL_STRINGS

def norm_lower(v: object) -> str:
    """Lowercased, trimmed string; empty if null-like."""
    return "" if is_blank(v) else str(v).strip().lower()

def first_nonempty(*vals) -> str:
    for v in vals:
        s = norm_lower(v)
        if s:
            return s
    return ""

def musicbrainz_top_tags(name_lower: str, top_n: int = 5, cache: dict | None = None):
    """Fetch up to top_n tags for an artist name from MusicBrainz with pacing + retries."""
    global _mb_last_ts
    if not name_lower:
        return []
    key = name_lower.strip().lower()
    if cache is not None and key in cache:
        return cache[key]

    params = {"query": f'artist:"{key}"', "fmt": "json", "limit": 1, "inc": "tags"}
    tries = 0
    while True:
        # enforce min interval
        now = time.time()
        delta = now - _mb_last_ts
        if delta < MB_MIN_INTERVAL:
            time.sleep(MB_MIN_INTERVAL - delta)

        try:
            r = MB_SESSION.get(
                "https://musicbrainz.org/ws/2/artist/",
                params=params, headers=UA, timeout=MB_TIMEOUT,
            )
            _mb_last_ts = time.time()

            # Retry on transient server errors
            if r.status_code in (429, 500, 502, 503, 504):
                tries += 1
                if tries > MB_MAX_RETRIES:
                    if cache is not None: cache[key] = []
                    return []
                # backoff + jitter
                wait = (MB_BACKOFF_FACTOR ** (tries - 1)) + random.uniform(0, 0.7)
                time.sleep(wait)
                continue

            # Non-transient error: give up for this artist
            if r.status_code != 200:
                if cache is not None: cache[key] = []
                return []

            js = r.json()
            if not js.get("artists"):
                if cache is not None: cache[key] = []
                return []
            best = sorted(js["artists"], key=lambda a: a.get("score", 0), reverse=True)[0]
            tags = best.get("tags") or []
            tags_sorted = sorted(tags, key=lambda t: t.get("count", 0), reverse=True)
            out = [t.get("name") for t in tags_sorted if t.get("name")][:top_n]
            if cache is not None: cache[key] = out
            return out

        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError):
            tries += 1
            if tries > MB_MAX_RETRIES:
                if cache is not None: cache[key] = []
                return []
            wait = (MB_BACKOFF_FACTOR ** (tries - 1)) + random.uniform(0, 0.7)
            time.sleep(wait)


# ---------- main ----------
def main():
    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)

    # Ensure expected columns exist (create empty if missing)
    for col in ["artist","l1","l2","l3","matched_name","genres_raw","source"]:
        if col not in df.columns:
            df[col] = ""

    out_rows = []
    n = 0

    # caches
    mb_cache: dict[str, list[str]] = {}
    last_q: str | None = None
    last_tags: list[str] | None = None

    for idx, row in df.iterrows():
        # read current values robustly
        genres_raw_val = row.get("genres_raw")
        source_val     = row.get("source")

        if is_blank(genres_raw_val):
            # choose a single search term
            q = first_nonempty(row.get("matched_name"),
                               row.get("artist"),
                               row.get("l1"),
                               row.get("l2"),
                               row.get("l3"))

            if q and last_q and last_tags is not None and q == last_q:
                tags = last_tags
            else:
                tags = musicbrainz_top_tags(q, top_n=5, cache=mb_cache)
                last_q, last_tags = q, tags

            if tags:
                row["genres_raw"] = ";".join(tags)
                row["source"] = "musicbrainz"

        out_rows.append(row)
        n += 1

        if n % SAVE_EVERY == 0:
            pd.DataFrame(out_rows).to_csv(
                OUTPUT_CSV, mode=("a" if OUTPUT_CSV.exists() else "w"),
                index=False, header=not OUTPUT_CSV.exists()
            )
            out_rows.clear()
            print(f"[Save] {n} rows processed…")

    if out_rows:
        pd.DataFrame(out_rows).to_csv(
            OUTPUT_CSV, mode=("a" if OUTPUT_CSV.exists() else "w"),
            index=False, header=not OUTPUT_CSV.exists()
        )

    print(f"[Done] Wrote MB-filled file → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
