# src/fetch_genres_raw_debug.py
import os, base64, requests, pandas as pd
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# ---------- Config ----------
INPUT_CSV  = Path("data/cleaned/split_4.csv")   # artist,l1,l2,l3,last_chart_year (lowercase)
OUTPUT_CSV = Path("data/enriched/artist_genres_raw4.csv")      # artist,l1,l2,l3,matched_name,spotify_artist_id,genres_raw,source,last_chart_year,matched_from
SAVE_EVERY = 20

load_dotenv(find_dotenv())
CID = os.getenv("SPOTIFY_CLIENT_ID"); CS = os.getenv("SPOTIFY_CLIENT_SECRET")
print("[Init] CID loaded?", bool(CID), "CS loaded?", bool(CS))

class RateLimit429(Exception):
    pass

# ---------- Spotify ----------
def get_token():
    print("[Auth] Requesting Spotify token...")
    basic = base64.b64encode(f"{CID}:{CS}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"}, timeout=20
    )
    print(f"[Auth] Response code: {r.status_code}")
    if r.status_code == 429:
        print("[Auth] Hit rate limit during token request.")
        raise RateLimit429()
    if r.status_code != 200:
        print("[Auth] Token request failed:", r.text)
    r.raise_for_status()
    token = r.json()["access_token"]
    print("[Auth] Token retrieved successfully.")
    return token

def search_exact_lower_spotify(token, name_lower: str, limit: int = 50):
    if not name_lower:
        return None
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization": f"Bearer {token}"},
        params={"q": f'artist:"{name_lower}"', "type": "artist", "limit": limit},
        timeout=20
    )
    if r.status_code == 429:
        print(f"[Search] Rate limit hit while searching '{name_lower}'")
        raise RateLimit429()
    if r.status_code != 200:
        print(f"[Search] Error {r.status_code} for '{name_lower}':", r.text[:200])
    r.raise_for_status()
    items = r.json().get("artists", {}).get("items", []) or []
    target = (name_lower or "").strip().lower()
    for it in items:
        if (it.get("name") or "").strip().lower() == target:
            return {"id": it.get("id"), "name": it.get("name")}
    return None

def get_artists_batch(token, ids):
    if not ids: return []
    print(f"[Batch] Fetching {len(ids)} artists...")
    r = requests.get(
        "https://api.spotify.com/v1/artists",
        headers={"Authorization": f"Bearer {token}"},
        params={"ids": ",".join(ids)}, timeout=20
    )
    if r.status_code == 429:
        print("[Batch] Rate limit hit while fetching artist batch.")
        raise RateLimit429()
    if r.status_code != 200:
        print("[Batch] Error fetching batch:", r.text[:200])
    r.raise_for_status()
    return r.json().get("artists", []) or []

# ---------- Main ----------
def main():
    print("[Main] Starting script...")
    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    print(f"[Main] Loading input CSV: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)
    print(f"[Main] Loaded {len(df)} rows.")

    out = []  # rows ready to flush

    try:
        token = get_token()

        # Phase 1: search each unique name once (minimize calls)
        print("[Phase 1] Collecting unique artist names...")
        cols = ["artist", "l1", "l2", "l3"]
        unique_names = set()
        for c in cols:
            unique_names |= set(str(x).strip().lower() for x in df[c].dropna().astype(str))
        print(f"[Phase 1] Found {len(unique_names)} unique names.")

        name_to_hit = {}
        for i, nm in enumerate(sorted(unique_names), start=1):
            if i % 50 == 0:
                print(f"[Phase 1] Processing artist {i}/{len(unique_names)}...")
            hit = search_exact_lower_spotify(token, nm)
            if hit:
                name_to_hit[nm] = hit
        print(f"[Phase 1] Completed. Matched {len(name_to_hit)} artists.")

        # Phase 2: batch-fetch genres (50 per call)
        print("[Phase 2] Fetching genres in batches...")
        unique_ids = sorted(set(hit["id"] for hit in name_to_hit.values() if hit.get("id")))
        id_to_genres, id_to_name = {}, {}
        for i in range(0, len(unique_ids), 50):
            print(f"[Phase 2] Fetching batch {i}-{i+50} of {len(unique_ids)}...")
            for a in get_artists_batch(token, unique_ids[i:i+50]):
                aid = a.get("id")
                if aid:
                    id_to_genres[aid] = (a.get("genres") or [])
                    id_to_name[aid]   = a.get("name")
        print(f"[Phase 2] Completed. Retrieved {len(id_to_genres)} genre lists.")

        # Phase 3: assemble rows (no API calls here)
        print("[Phase 3] Assembling final output rows...")
        n = 0
        last_q_used = None
        last_payload = None
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            if idx % 100 == 0:
                print(f"[Phase 3] Processed {idx}/{len(df)} rows...")

            artist = str(row.get("artist") or "").strip().lower()
            qvals  = [
                artist,
                str(row.get("l1") or "").strip().lower(),
                str(row.get("l2") or "").strip().lower(),
                str(row.get("l3") or "").strip().lower(),
            ]
            last_chart_year = row.get("last_chart_year", "")

            if last_q_used and last_payload and (last_q_used in qvals):
                idx2 = qvals.index(last_q_used)
                out.append({
                    "artist": artist,
                    "l1": qvals[1], "l2": qvals[2], "l3": qvals[3],
                    "matched_name": last_payload["matched_name"],
                    "spotify_artist_id": last_payload["spotify_id"],
                    "genres_raw": ";".join(last_payload["genres_raw"]),
                    "source": last_payload["source"],
                    "matched_from": ["artist","l1","l2","l3"][idx2],
                    "last_chart_year": last_chart_year
                })
            else:
                matched_name = None
                spotify_id   = None
                genres_raw   = []
                source       = "none"
                matched_from = None
                q_used       = None

                for idx2, q in enumerate(qvals):
                    hit = name_to_hit.get(q)
                    if not hit: continue
                    aid = hit.get("id")
                    g   = id_to_genres.get(aid, [])
                    matched_name = hit.get("name")
                    spotify_id   = aid
                    matched_from = ["artist","l1","l2","l3"][idx2]
                    q_used       = q
                    if g:
                        genres_raw = g[:5]
                        source = "spotify"
                    break

                last_q_used = q_used
                last_payload = {
                    "matched_name": matched_name,
                    "spotify_id": spotify_id,
                    "genres_raw": genres_raw,
                    "source": source
                }

                out.append({
                    "artist": artist,
                    "l1": qvals[1], "l2": qvals[2], "l3": qvals[3],
                    "matched_name": matched_name,
                    "spotify_artist_id": spotify_id,
                    "genres_raw": ";".join(genres_raw),
                    "source": source,
                    "matched_from": matched_from,
                    "last_chart_year": last_chart_year
                })

            n += 1
            if n % SAVE_EVERY == 0:
                pd.DataFrame(out).to_csv(
                    OUTPUT_CSV, mode=("a" if OUTPUT_CSV.exists() else "w"),
                    index=False, header=not OUTPUT_CSV.exists()
                )
                print(f"[Save] Flushed {n} rows to disk.")
                out.clear()

        if out:
            pd.DataFrame(out).to_csv(
                OUTPUT_CSV, mode=("a" if OUTPUT_CSV.exists() else "w"),
                index=False, header=not OUTPUT_CSV.exists()
            )
        print(f"[Done] Wrote all results to {OUTPUT_CSV}")

    except RateLimit429:
        if out:
            pd.DataFrame(out).to_csv(
                OUTPUT_CSV, mode=("a" if OUTPUT_CSV.exists() else "w"),
                index=False, header=not OUTPUT_CSV.exists()
            )
            print(f"[429] Partial save complete → {OUTPUT_CSV}")
        else:
            print("[429] No new rows since last save. Exiting.")
        return

if __name__ == "__main__":
    main()
