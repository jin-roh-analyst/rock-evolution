import time, requests, pandas as pd
from spotify_auth import get_token

def search_track(token, title, artist):
    q = f'track:"{title}" artist:"{artist}"'
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization": f"Bearer {token}"},
        params={"q": q, "type": "track", "limit": 1},
        timeout=30
    )
    if r.status_code == 401:  # token expired
        raise RuntimeError("TOKEN_EXPIRED")
    r.raise_for_status()
    items = r.json().get("tracks", {}).get("items", [])
    return items[0] if items else None

def get_artist_genres(token, artist_id):
    r = requests.get(
        f"https://api.spotify.com/v1/artists/{artist_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30
    )
    r.raise_for_status()
    return r.json().get("genres", [])

def map_main_genre(genres):
    text = " ".join(genres).lower()
    if "rock" in text: return "Rock"
    if "metal" in text: return "Metal"
    if "hip hop" in text or "rap" in text: return "Hip-hop"
    if "pop" in text: return "Pop"
    if "country" in text: return "Country"
    if "r&b" in text or "soul" in text: return "R&B/Soul"
    return "Other"

if __name__ == "__main__":
    token = get_token()
    df = pd.read_csv("data/raw/billboard_hot-100_2015-01-03.csv")  # try the file you just saved
    out_rows = []
    for _, row in df.iterrows():
        title, artist = row["title"], row["artist"]
        tr = search_track(token, title, artist)
        if not tr:
            out_rows.append({**row, "spotify_track_id": None, "artist_genres": None, "genre_main": None})
            continue
        artist_id = tr["artists"][0]["id"]
        genres = get_artist_genres(token, artist_id)
        out_rows.append({
            **row,
            "spotify_track_id": tr["id"],
            "artist_genres": ";".join(genres),
            "genre_main": map_main_genre(genres)
        })
        time.sleep(0.1)  # polite throttle
    out = "data/cleaned/billboard_hot-100_2015-01-03_enriched.csv"
    pd.DataFrame(out_rows).to_csv(out, index=False)
    print("Wrote", out)
