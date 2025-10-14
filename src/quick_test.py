# src/quick_spotify_test.py
import os, base64, requests, time
from dotenv import load_dotenv

load_dotenv()
CID = os.getenv("SPOTIFY_CLIENT_ID")
CS  = os.getenv("SPOTIFY_CLIENT_SECRET")
if not CID or not CS:
    raise SystemExit("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env")

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
    """Return Spotify artist object only if item.name.lower() == name_lower."""
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization": f"Bearer {token}"},
        params={"q": f'artist:"{name_lower}"', "type": "artist", "limit": limit},
        timeout=20,
    )
    r.raise_for_status()
    items = r.json().get("artists", {}).get("items", []) or []
    for it in items:
        if (it.get("name") or "").lower().strip() == name_lower:
            return it
    return None

def get_spotify_artist(token, artist_id):
    r = requests.get(
        f"https://api.spotify.com/v1/artists/{artist_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()

def musicbrainz_top_tags(name_lower: str, top_n: int = 5):
    """Return up to top_n MusicBrainz tags for the given artist name (lowercase)."""
    # MB courtesy: ~1 req/sec and a UA string
    time.sleep(1.1)
    r = requests.get(
        "https://musicbrainz.org/ws/2/artist/",
        params={"query": f'artist:"{name_lower}"', "fmt": "json"},
        headers={"User-Agent": "rock-evolution/1.0 (contact: you@example.com)"},
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

def main():
    token = get_token()
    q = input("🎵 Enter artist name (lowercase, exact): ").strip().lower()

    sp_artist = search_exact_lower_spotify(token, q)
    if not sp_artist:
        print(f"❌ No exact (lowercase) Spotify match for '{q}'.")
        return

    full = get_spotify_artist(token, sp_artist["id"])
    sp_genres = full.get("genres", []) or []

    print(f"\n✅ Spotify match: {full.get('name')}  (ID: {full.get('id')})")
    if sp_genres:
        print("Spotify genres:", ", ".join(sp_genres))
    else:
        print("Spotify genres: (none)")
        mb = musicbrainz_top_tags(q, top_n=5)
        print("MusicBrainz top tags:", ", ".join(mb) if mb else "(none)")

if __name__ == "__main__":
    main()
