# src/fetch_genres_raw.py
import os, base64, time, random, requests, pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ---------- Config ----------
INPUT_CSV  = Path("data/cleaned/unique_artists_levels.csv")  # columns: artist,l1,l2,l3,last_chart_year (all lowercase)
OUTPUT_CSV = Path("data/enriched/artist_genres_raw.csv")     # artist,l1,l2,l3,matched_name,spotify_artist_id,genres_raw,source,last_chart_year
SAVE_EVERY = 20

# Global pacing + retries (for 429 handling)
SESSION = requests.Session()
MIN_INTERVAL      = 0.35   # seconds between Spotify calls (~3 req/s); tune if needed
MAX_RETRY_AFTER   = 30     # cap absurd Retry-After values
MAX_RETRIES       = 5      # don't loop forever on a single request
_last_call_ts     = 0.0

load_dotenv()
CID = os.getenv("SPOTIFY_CLIENT_ID")
CS  = os.getenv("SPOTIFY_CLIENT_SECRET")
if not CID or not CS:
    raise SystemExit("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env")

# ---------- Spotify ----------
def get_token():
    basic = base64.b64encode(f"{CID}:{CS}".encode()).decode()
    r = SESSION.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def sp_get(token, url, params=None, timeout=20):
    """Rate-limited GET with 401/429 handling, jitter, and retry cap."""
    global _last_call_ts
    backoff = 1.5
    tries = 0
    while True:
        # global min interval (simple leaky bucket)
        now = time.time()
        delta = now - _last_call_ts
        if delta < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - delta)

        r = SESSION.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=timeout)
        _last_call_ts = time.time()

        if r.status_code == 429:
            retry_hdr = r.headers.get("Retry-After", "2")
            try:
                retry = int(float(retry_hdr))
            except Exception:
                retry = 2
            wait = min(retry, MAX_RETRY_AFTER)
            wait = wait * (backoff ** tries) + random.uniform(0, 1.0)  # backoff + jitter
            print(f"[429] {url} → sleeping {round(wait,1)}s (try {tries+1}/{MAX_RETRIES})")
            time.sleep(wait)
            tries += 1
            if tries >= MAX_RETRIES:
                # Give up on this request; let caller proceed with empty
                return {}
            continue

        if r.status_code == 401:
            # Signal caller to refresh token once
            raise RuntimeError("TOKEN_EXPIRED")

        r.raise_for_status()
        return r.json()

def search_exact_lower_spotify(token, name_lower: str, limit: int = 50):
    """Return Spotify artist object only if item.name.lower() == name_lower."""
    if not name_lower:
        return None
    data = sp_get(token, "https://api.spotify.com/v1/search",
                  params={"q": f'artist:"{name_lower}"', "type": "artist", "limit": limit})
    items = (data.get("artists", {}) or {}).get("items", []) if data else []
    target = (name_lower or "").strip().lower()
    for it in items:
        if (it.get("name") or "").strip().lower() == target:
            return it
    return None

def get_spotify_artist(token, artist_id):
    data = sp_get(token, f"https://api.spotify.com/v1/artists/{artist_id}")
    return data or {}

# ---------- MusicBrainz (fallback) ----------
UA = {"User-Agent": "rock-evolution/1.0 (contact: your-email@example.com)"}

def musicbrainz_top_tags(name_lower: str, top_n: int = 5):
    """Get up to top_n tags for an artist name from MusicBrainz."""
    if not name_lower:
        return []
    time.sleep(1.1)  # be nice: ~1 req/sec
    r = SESSION.get(
        "https://musicbrainz.org/ws/2/artist/",
        params={"query": f'artist:"{name_lower}"', "fmt": "json"},
        headers=UA,
        timeout=20,
    )
    if r.status_code != 200:
        return []
    js = r.json()
    if not js.get("artists"):
        return []
    best = sorted(js["artists"], key=lambda a: a.get("score", 0), reverse=True)[0]
    tags = best.get("tags") or []
    tags_sorted = sorted(tags, key=lambda t: t.get("count", 0), reverse=True)
    return [t.get("name") for t in tags_sorted if t.get("name")][:top_n]

# ---------- Main ----------
def main():
    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)  # expects: artist,l1,l2,l3,last_chart_year

    # resume support
    done = set()
    if OUTPUT_CSV.exists():
        prev = pd.read_csv(OUTPUT_CSV, usecols=["artist"])
        done = set(prev["artist"].astype(str))

    token = get_token()
    buffer, n = [], 0

    # minimal cache to skip repeated names (artist/l1/l2/l3)
    cache = {}  # key: exact search term (lowercase); val: (matched_name, spotify_id, genres_raw_list, source)

    for _, row in df.iterrows():
        artist = (str(row.get("artist") or "").strip().lower())
        if not artist or artist in done:
            continue

        last_chart_year = row.get("last_chart_year", "")
        match_source = None

        # Try artist, then l1, l2, l3 — stop at first exact lowercase match
        queries = [
            artist,
            str(row.get("l1") or "").strip().lower(),
            str(row.get("l2") or "").strip().lower(),
            str(row.get("l3") or "").strip().lower(),
        ]

        matched_name = None
        spotify_id   = None
        genres_raw   = []
        source       = "none"

        used_q = None
        for q in queries:
            if not q:
                continue

            # cache hit?
            if q in cache:
                matched_name, spotify_id, genres_raw, source = cache[q]
                used_q = q
                match_source = ["artist","l1","l2","l3"][queries.index(q)]
                break

            # otherwise search
            try:
                match = search_exact_lower_spotify(token, q)
            except RuntimeError:  # token expired
                token = get_token()
                match = search_exact_lower_spotify(token, q)
            except requests.RequestException:
                match = None

            if match:
                matched_name = match.get("name")
                spotify_id   = match.get("id")
                used_q = q
                match_source = ["artist","l1","l2","l3"][queries.index(q)]

                # fetch genres (Spotify first)
                try:
                    full = get_spotify_artist(token, spotify_id)
                    genres_raw = (full.get("genres") or [])[:5]
                except RuntimeError:
                    token = get_token()
                    full = get_spotify_artist(token, spotify_id)
                    genres_raw = (full.get("genres") or [])[:5]
                except requests.RequestException:
                    genres_raw = []

                if genres_raw:
                    source = "spotify"
                else:
                    mb = musicbrainz_top_tags(matched_name.lower() if matched_name else q, top_n=5)
                    if mb:
                        genres_raw = mb
                        source = "musicbrainz"
                    else:
                        source = "none"

                cache[q] = (matched_name, spotify_id, genres_raw, source)
                break

        # If no match at all and no cache hit, also cache the miss to skip repeats
        if used_q is None:
            cache[artist] = (None, None, [], "none")

        buffer.append({
            "artist": artist,
            "l1": queries[1] or "",
            "l2": queries[2] or "",
            "l3": queries[3] or "",
            "matched_name": matched_name,
            "spotify_artist_id": spotify_id,
            "genres_raw": ";".join(genres_raw),
            "source": source,
            "matched_from": match_source,
            "last_chart_year": last_chart_year
        })

        n += 1
        if n % SAVE_EVERY == 0:
            pd.DataFrame(buffer).to_csv(
                OUTPUT_CSV,
                mode=("a" if OUTPUT_CSV.exists() else "w"),
                index=False,
                header=not OUTPUT_CSV.exists(),
            )
            buffer.clear()
            print(f"Saved {n} rows…")

    if buffer:
        pd.DataFrame(buffer).to_csv(
            OUTPUT_CSV,
            mode=("a" if OUTPUT_CSV.exists() else "w"),
            index=False,
            header=not OUTPUT_CSV.exists(),
        )

    print(f"Done. Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
