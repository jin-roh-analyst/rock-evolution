# src/fetch_genres_raw.py
import os, base64, time, requests, pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ---------- Config ----------
INPUT_CSV  = Path("data/cleaned/artists_levels_year.csv")  # columns: artist,l1,l2,l3,last_chart_year (all lowercase)
OUTPUT_CSV = Path("data/enriched/artist_genres_raw.csv")          # artist,l1,l2,l3,matched_name,spotify_artist_id,genres_raw,source,last_chart_year
SAVE_EVERY = 100

load_dotenv()
CID = os.getenv("SPOTIFY_CLIENT_ID")
CS  = os.getenv("SPOTIFY_CLIENT_SECRET")
if not CID or not CS:
    raise SystemExit("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env")

# ---------- Spotify ----------
def get_token():
    basic = base64.b64encode(f"{CID}:{CS}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def search_exact_lower_spotify(token, name_lower: str, limit: int = 50):
    """ Return Spotify artist object only if item.name.lower() == name_lower. """
    if not name_lower:
        return None
    for _ in range(3):
        r = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": f'artist:"{name_lower}"', "type": "artist", "limit": limit},
            timeout=20,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2")); time.sleep(wait + 1); continue
        if r.status_code == 401:
            raise RuntimeError("TOKEN_EXPIRED")
        r.raise_for_status()
        items = r.json().get("artists", {}).get("items", []) or []
        target = name_lower.strip().lower()
        for it in items:
            if (it.get("name") or "").strip().lower() == target:
                return it
        return None
    return None

def get_spotify_artist(token, artist_id):
    for _ in range(3):
        r = requests.get(
            f"https://api.spotify.com/v1/artists/{artist_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2")); time.sleep(wait + 1); continue
        if r.status_code == 401:
            raise RuntimeError("TOKEN_EXPIRED")
        r.raise_for_status()
        return r.json()
    return {}

# ---------- MusicBrainz (fallback) ----------
UA = {"User-Agent": "rock-evolution/1.0 (contact: your-email@example.com)"}

def musicbrainz_top_tags(name_lower: str, top_n: int = 5):
    """ Get up to top_n tags for an artist name from MusicBrainz. """
    if not name_lower:
        return []
    time.sleep(1.1)  # be nice: ~1 req/sec
    r = requests.get(
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

    # ---- minimal cache to skip repeated names (artist/l1/l2/l3) ----
    cache = {}  # key: exact search term (lowercase); val: (matched_name, spotify_id, genres_raw_list, source)

    for _, row in df.iterrows():
        artist = (str(row.get("artist") or "").strip().lower())
        if not artist or artist in done:
            continue

        last_chart_year = row.get("last_chart_year", "")
        match_source = None  # <---- added

        # 1) Try artist, then l1, then l2, then l3 — stop at first exact lowercase match
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
            # quick cache hit
            if q in cache:
                matched_name, spotify_id, genres_raw, source = cache[q]
                used_q = q
                match_source = ["artist","l1","l2","l3"][queries.index(q)]  # <---- added
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
                match_source = ["artist","l1","l2","l3"][queries.index(q)]  # <---- added
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
                # store in cache
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
            "matched_from": match_source,          # <---- added
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
