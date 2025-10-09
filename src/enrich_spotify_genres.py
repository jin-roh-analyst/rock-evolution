import os, time, base64, math, json, requests, pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ----------------- Config -----------------
INPUT_CSV   = Path("data/cleaned/unique_artists_cleanlevels.csv")
OUTPUT_CSV  = Path("data/enriched/artist_genres_cache_levels.csv")   # append-only cache

# Speed knobs
LIGHT_MODE          = True   # True = only Direct + Related (much faster). False = also Top-Tracks + Recs
BASE_SLEEP          = 0.01   # small pacing between search attempts
BATCH_SAVE_EVERY    = 50     # save progress frequently
K_MIN_TAGS          = 6      # min total tags for inference
RATIO_MIN           = 2.0    # top bucket must be >= 2x next

# Bounded caps for deeper inference
MAX_COARTISTS       = 20     # unique co-artists to inspect from top-tracks
MAX_REC_ARTISTS     = 50     # unique artists from recommendations to inspect

# Simple local caches (dramatic speed-ups on reruns)
CACHE_DIR = Path("cache")
CACHE_ARTIST      = CACHE_DIR / "artists"           # /{id}.json
CACHE_RELATED     = CACHE_DIR / "related"           # /{id}.json
CACHE_TOPTRACKS   = CACHE_DIR / "top_tracks"        # /{id}.json
CACHE_RECS        = CACHE_DIR / "recs"              # /{id}.json
CACHE_SEARCH      = CACHE_DIR / "search_name_to_id.json"  # flat dict

for p in [CACHE_ARTIST, CACHE_RELATED, CACHE_TOPTRACKS, CACHE_RECS]:
    p.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------
load_dotenv()
CID = os.getenv("SPOTIFY_CLIENT_ID")
CS  = os.getenv("SPOTIFY_CLIENT_SECRET")

# ---------- Auth ----------
def get_token():
    basic = base64.b64encode(f"{CID}:{CS}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}",
               "Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post("https://accounts.spotify.com/api/token",
                      headers=headers, data={"grant_type":"client_credentials"}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

# --------- GET with 429/401 handling; allow ok_404 ----------
def sp_get(token, url, params=None, max_retries=3, ok_404=False):
    tries = 0
    while True:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "2"))
            time.sleep(retry + 1)
            tries += 1
            if tries > max_retries:
                r.raise_for_status()
            continue
        if r.status_code == 401:
            raise RuntimeError("TOKEN_EXPIRED")
        if r.status_code == 404 and ok_404:
            return {}
        r.raise_for_status()
        return r.json()

# ---------- Genre mapping ----------
BUCKETS = {
    "Rock": ["rock","alt rock","alternative rock","punk","post-punk","grunge","emo","shoegaze","garage rock","hard rock","classic rock","indie rock","math rock","prog rock","pop punk","punk rock","blues rock","stoner rock"],
    "Metal": ["metal","heavy metal","black metal","death metal","doom","thrash","nu metal","metalcore","deathcore","sludge","power metal","symphonic metal","prog metal"],
    "Pop": ["pop","electropop","synthpop","dance pop","teen pop","bedroom pop","hyperpop","bubblegum pop","kawaii future bass"],
    "HipHop": ["hip hop","hip-hop","rap","trap","drill","boom bap","gangsta rap","cloud rap"],
    "RnB": ["r&b","rnb","soul","neo soul","funk","new jack swing","quiet storm"],
    "Country": ["country","americana","bluegrass","alt-country"],
    "Electronic": ["electronic","edm","house","techno","trance","dubstep","dnb","drum and bass","breakbeat","idm","downtempo","electro","garage","grime","future bass"],
    "Latin": ["latin","reggaeton","urbano","urbano latino","banda","corridos","mariachi","cumbia","bachata","salsa","merengue","vallenato","latino","trap latino"],
    "Reggae": ["reggae","dancehall","dub reggae","roots reggae","ska","rocksteady"],
    "Jazz": ["jazz","bebop","swing","cool jazz","free jazz","fusion","hard bop","latin jazz"],
    "Classical": ["classical","orchestra","baroque","romantic era","opera","symphony","chamber","choral"],
    "Folk": ["folk","singer-songwriter","celtic","traditional folk","appalachian"],
    "Blues": ["blues","delta blues","chicago blues","electric blues"],
    "Gospel": ["gospel","ccm","worship","christian"],
    "Afrobeat": ["afrobeat","afrobeats","naija","nigerian pop","amapiano"],
    "KPop": ["k-pop","k pop","korean pop"],
    "JPop": ["j-pop","j pop","anisong","j-rock","j rock"],
    "World": ["bollywood","arab","turkish","balkan","fado","flamenco","tango","carnatic","hindustani","qawwali","enka","c-pop","c pop","mandopop","cantopop","african","highlife","mbaqanga"]
}
SPECIAL_FIRST = ["KPop", "JPop", "Afrobeat"]

def _norm(s: str) -> str:
    return (s or "").lower()

def bucket_score(genres_list):
    text = _norm(" ".join(genres_list))
    scores = {k:0 for k in BUCKETS.keys()}

    # early exits
    for special in SPECIAL_FIRST:
        for kw in BUCKETS[special]:
            if kw in text:
                return special, scores

    # christian X => map to X if present, else Gospel
    if "christian" in text or "worship" in text or "ccm" in text:
        if any(kw in text for kw in BUCKETS["Rock"]):   return "Rock", scores
        if any(kw in text for kw in BUCKETS["HipHop"]): return "HipHop", scores
        if any(kw in text for kw in BUCKETS["Pop"]):    return "Pop", scores
        return "Gospel", scores

    # indie X nudges
    if "indie rock" in text: scores["Rock"] += 2
    if "indie pop"  in text: scores["Pop"]  += 2

    # generic scoring
    for bucket, kws in BUCKETS.items():
        for kw in kws:
            if kw in text:
                scores[bucket] += 1

    # Pop vs Rock tweak
    if scores["Pop"] > 0 and scores["Rock"] > 0:
        if any(term in text for term in ["punk","metal","grunge","emo","hard rock","pop punk","punk rock"]):
            scores["Rock"] += 1
        elif "pop rock" in text:
            scores["Rock"] += 1
        else:
            scores["Pop"] += 1

    top = max(scores, key=lambda k: scores[k])
    if scores[top] == 0:
        return None, scores
    return top, scores

def map_genre_main(genres_list):
    if not genres_list:
        return None
    top, scores = bucket_score(genres_list)
    return top if top else "Other"

# ---------- Disk cache helpers ----------
def _read_json(path: Path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None

def _write_json(path: Path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception:
        pass

# ---------- Spotify helpers (cached) ----------
def search_artist(token, q):
    if not q or not str(q).strip():
        return None

    # cache by query
    cache_map = {}
    if CACHE_SEARCH.exists():
        try:
            cache_map = json.load(open(CACHE_SEARCH, "r"))
        except Exception:
            cache_map = {}

    key = q.strip().lower()
    cached_id = cache_map.get(key)
    if cached_id:  # return minimal faux item
        return {"id": cached_id, "name": q}

    data = sp_get(token, "https://api.spotify.com/v1/search",
                  params={"q": f'artist:"{q}"', "type": "artist", "limit": 1})
    items = data.get("artists", {}).get("items", [])
    if not items:
        return None

    artist = items[0]
    cache_map[key] = artist["id"]
    _write_json(CACHE_SEARCH, cache_map)
    return artist

def get_artist(token, artist_id):
    artist_id = (artist_id or "").strip()
    p = CACHE_ARTIST / f"{artist_id}.json"
    cached = _read_json(p)
    if cached is not None:
        return cached
    data = sp_get(token, f"https://api.spotify.com/v1/artists/{artist_id}", ok_404=True) or {}
    _write_json(p, data)
    return data

def get_related_artists(token, artist_id):
    artist_id = (artist_id or "").strip()
    p = CACHE_RELATED / f"{artist_id}.json"
    cached = _read_json(p)
    if cached is not None:
        return cached
    data = sp_get(token, f"https://api.spotify.com/v1/artists/{artist_id}/related-artists", ok_404=True) or {}
    _write_json(p, data)
    return data

def get_top_tracks(token, artist_id, market="US"):
    artist_id = (artist_id or "").strip()
    p = CACHE_TOPTRACKS / f"{artist_id}.json"
    cached = _read_json(p)
    if cached is not None:
        return cached
    data = sp_get(token, f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks", params={"market": market}, ok_404=True) or {}
    _write_json(p, data)
    return data

def get_recommendations(token, seed_artist, limit=100):
    seed_artist = (seed_artist or "").strip()
    p = CACHE_RECS / f"{seed_artist}_{limit}.json"
    cached = _read_json(p)
    if cached is not None:
        return cached
    data = sp_get(token, "https://api.spotify.com/v1/recommendations",
                  params={"seed_artists": seed_artist, "limit": limit}, ok_404=True) or {}
    _write_json(p, data)
    return data

def get_artists_batch(token, ids):
    if not ids: return []
    data = sp_get(token, "https://api.spotify.com/v1/artists", params={"ids": ",".join(ids)}, ok_404=True) or {}
    return data.get("artists", [])

# ---------- Inference ----------
def aggregate_and_decide(genres_accum):
    if not genres_accum:
        return None, 0.0
    top = map_genre_main(genres_accum)
    if not top:
        return None, 0.0
    # ratio check
    _, scores = bucket_score(genres_accum)
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best, best_s = ordered[0]
    next_s = ordered[1][1] if len(ordered) > 1 else 0
    ratio = (best_s / max(1, next_s)) if next_s > 0 else math.inf
    total_hits = sum(1 for g in genres_accum if g)
    if total_hits >= K_MIN_TAGS and ratio >= RATIO_MIN:
        return best, min(1.0, 0.5 + 0.1 * best_s)
    return None, 0.0

def infer_genres_for_artist(token, artist_id):
    # Step 0 — direct
    a = get_artist(token, artist_id)
    direct_genres = a.get("genres", []) or []
    if direct_genres:
        return {
            "genres_raw": direct_genres,
            "genre_main": map_genre_main(direct_genres),
            "inferred_from": "direct",
            "confidence": 1.0
        }

    # Step 1 — related artists (1 call)  [FAST]
    rel = get_related_artists(token, artist_id)
    rel_genres = []
    for ra in rel.get("artists", []) or []:
        rel_genres.extend(ra.get("genres", []) or [])
    g, conf = aggregate_and_decide(rel_genres)
    if g:
        return {
            "genres_raw": sorted(set(rel_genres)),
            "genre_main": g,
            "inferred_from": "related",
            "confidence": min(conf, 0.8)
        }

    # Optional deeper steps (slower). Skip in LIGHT_MODE.
    if not LIGHT_MODE:
        # Step 2 — top tracks → co-artists (batched)
        tt = get_top_tracks(token, artist_id, market="US")
        co_ids = set()
        for tr in tt.get("tracks", []) or []:
            for ar in tr.get("artists", []) or []:
                aid = ar.get("id")
                if aid and aid != artist_id:
                    co_ids.add(aid)
        co_list = list(co_ids)[:MAX_COARTISTS]
        co_genres = []
        for i in range(0, len(co_list), 50):
            batch = co_list[i:i+50]
            artists = get_artists_batch(token, batch)
            for ar in artists:
                co_genres.extend(ar.get("genres", []) or [])
        g2, conf2 = aggregate_and_decide(co_genres)
        if g2:
            return {
                "genres_raw": sorted(set(co_genres)),
                "genre_main": g2,
                "inferred_from": "top_tracks",
                "confidence": min(conf2, 0.7)
            }

        # Step 3 — recommendations (batched)
        recs = get_recommendations(token, artist_id, limit=100)
        rec_ids = []
        for tr in recs.get("tracks", []) or []:
            for ar in tr.get("artists", []) or []:
                aid = ar.get("id")
                if aid and aid != artist_id:
                    rec_ids.append(aid)
        rec_ids = list(dict.fromkeys(rec_ids))[:MAX_REC_ARTISTS]
        rec_genres = []
        for i in range(0, len(rec_ids), 50):
            batch = rec_ids[i:i+50]
            artists = get_artists_batch(token, batch)
            for ar in artists:
                rec_genres.extend(ar.get("genres", []) or [])
        g3, conf3 = aggregate_and_decide(rec_genres)
        if g3:
            return {
                "genres_raw": sorted(set(rec_genres)),
                "genre_main": g3,
                "inferred_from": "recs",
                "confidence": min(conf3, 0.6)
            }

    # Give up gracefully
    return {"genres_raw": [], "genre_main": None, "inferred_from": "none", "confidence": 0.0}

# ---------- Main loop (artist → L1 → L2 → L3) ----------
def main():
    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)  # expects: artist,l1,l2,l3

    # resume cache
    cache_cols = ["artist","l1","l2","l3","matched_name","spotify_artist_id",
                  "genres_raw","genre_main","inferred_from","confidence"]
    if OUTPUT_CSV.exists():
        done_df = pd.read_csv(OUTPUT_CSV, usecols=["artist"])
        done = set(done_df["artist"].astype(str))
    else:
        done = set()

    token = get_token()
    buffer = []
    processed = 0

    for _, row in df.iterrows():
        artist = str(row.get("artist", "")).strip()
        if not artist or artist in done:
            continue

        queries = [artist, row.get("l1"), row.get("l2"), row.get("l3")]
        match = None
        for q in queries:
            try:
                match = search_artist(token, q)
            except RuntimeError:
                token = get_token()
                match = search_artist(token, q)
            except requests.RequestException:
                match = None
            if match:
                break
            time.sleep(BASE_SLEEP)

        if not match:
            buffer.append({
                "artist": artist,
                "l1": row.get("l1"), "l2": row.get("l2"), "l3": row.get("l3"),
                "matched_name": None, "spotify_artist_id": None,
                "genres_raw": "", "genre_main": None,
                "inferred_from": "none", "confidence": 0.0
            })
        else:
            artist_id = (match.get("id") or "").strip()
            matched_name = match.get("name")
            try:
                info = infer_genres_for_artist(token, artist_id)
            except RuntimeError:
                token = get_token()
                info = infer_genres_for_artist(token, artist_id)

            buffer.append({
                "artist": artist,
                "l1": row.get("l1"), "l2": row.get("l2"), "l3": row.get("l3"),
                "matched_name": matched_name, "spotify_artist_id": artist_id,
                "genres_raw": ";".join(sorted(set(info["genres_raw"]))),
                "genre_main": info["genre_main"],
                "inferred_from": info["inferred_from"],
                "confidence": round(float(info["confidence"]), 3)
            })

        processed += 1
        if processed % BATCH_SAVE_EVERY == 0:
            pd.DataFrame(buffer).to_csv(OUTPUT_CSV, mode="a", index=False,
                                        header=not OUTPUT_CSV.exists())
            print(f"Saved {processed} rows …")
            buffer = []

    if buffer:
        pd.DataFrame(buffer).to_csv(OUTPUT_CSV, mode="a", index=False,
                                    header=not OUTPUT_CSV.exists())

    print(f"Done. Cache at {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
