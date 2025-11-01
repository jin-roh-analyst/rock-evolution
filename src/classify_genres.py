# classify_genres.py
import sys, math, pandas as pd

# --------- High-level buckets and keywords ---------
BUCKETS = {
    "Rock": ["rock","alt rock","alternative rock","punk","post-punk","grunge","emo","shoegaze",
             "garage rock","hard rock","classic rock","indie rock","math rock","prog rock",
             "pop punk","punk rock","blues rock","stoner rock","psych rock","post rock","metal","heavy metal","black metal","death metal","doom","thrash","nu metal",
              "metalcore","deathcore","sludge","power metal","symphonic metal","prog metal"],
    "Pop": ["pop","electropop","synthpop","dance pop","teen pop","bedroom pop","hyperpop",
            "bubblegum pop","indie pop","k-pop","j-pop","c-pop","mandopop","cantopop"],
    "HipHop": ["hip hop","hip-hop","rap","trap","drill","boom bap","gangsta rap","cloud rap"],
    "RnB": ["r&b","rnb","soul","neo soul","funk","new jack swing","quiet storm"],
    "Country": ["country","americana","bluegrass","alt-country"],
    "Electronic": ["electronic","edm","house","techno","trance","dubstep","dnb","drum and bass",
                   "breakbeat","idm","downtempo","electro","garage","grime","future bass"],
    "Latin": ["latin","reggaeton","urbano","banda","corridos","mariachi","cumbia","bachata",
              "salsa","merengue","vallenato","trap latino","urbano latino"],
    "Reggae": ["reggae","dancehall","dub reggae","roots reggae","ska","rocksteady"],
    "Jazz": ["jazz","bebop","swing","cool jazz","free jazz","fusion","hard bop","latin jazz"],
    "Classical": ["classical","orchestra","baroque","romantic era","opera","symphony","chamber","choral"],
    "Folk": ["folk","singer-songwriter","celtic","traditional folk","appalachian"],
    "Blues": ["blues","delta blues","chicago blues","electric blues"],
    "Gospel": ["gospel","ccm","worship","christian"],
    "Afrobeat": ["afrobeat","afrobeats","naija","amapiano","nigerian pop"],
    "World": ["bollywood","arab","turkish","balkan","fado","flamenco","tango","carnatic",
              "hindustani","qawwali","enka","african","highlife","mbaqanga"]
}

# quick early-exit “labels” that shouldn’t be overshadowed
SPECIAL_FIRST = ["Afrobeat"]

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _bucket_score(tags):
    text = _norm(" ".join(tags))
    if not text:
        return None

    # Early exits (e.g., Afrobeat)
    for special in SPECIAL_FIRST:
        for kw in BUCKETS[special]:
            if kw in text:
                return special

    # “Christian X” → X if present, else Gospel
    if any(k in text for k in ["christian","worship","ccm"]):
        if any(k in text for k in BUCKETS["Rock"]):   return "Rock"
        if any(k in text for k in BUCKETS["HipHop"]): return "HipHop"
        if any(k in text for k in BUCKETS["Pop"]):    return "Pop"
        return "Gospel"

    # Scoring
    scores = {k: 0 for k in BUCKETS}
    # Slight nudges for “indie rock/pop”
    if "indie rock" in text: scores["Rock"] += 2
    if "indie pop"  in text: scores["Pop"]  += 2

    for bucket, kws in BUCKETS.items():
        for kw in kws:
            if kw in text:
                scores[bucket] += 1

    # Pop vs Rock tiebreaker: punk/metal-ish cues → Rock
    if scores["Pop"] > 0 and scores["Rock"] > 0:
        if any(t in text for t in ["punk","metal","grunge","emo","hard rock","pop punk","punk rock","shoegaze"]):
            scores["Rock"] += 1
        elif "pop rock" in text:
            scores["Rock"] += 1
        else:
            scores["Pop"] += 1

    top = max(scores, key=lambda k: scores[k])
    return top if scores[top] > 0 else None

def pick_main(genres_raw: str) -> str:
    """
    genres_raw: semicolon-separated tags, e.g. "alternative rock;indie rock;shoegaze;indietronica;rock"
    returns a single high-level bucket. Empty if no genres provided.
    """
    if not isinstance(genres_raw, str) or not genres_raw.strip():
        return ""  # leave blank if no genres_raw
    tags = [_norm(t) for t in genres_raw.split(";") if _norm(t)]
    if not tags:
        return ""
    bucket = _bucket_score(tags)
    return bucket if bucket else ""


def main(in_path: str, out_path: str):
    df = pd.read_csv(in_path)
    if "genres_raw" not in df.columns:
        raise SystemExit("Input CSV must have a 'genres_raw' column.")
    df["genre_main"] = df["genres_raw"].apply(pick_main)
    df.to_csv(out_path, index=False)
    print(f"Saved with genre_main → {out_path}")

if __name__ == "__main__":
    # Usage:
    #   python classify_genres.py merged.csv merged_with_main.csv
    inp = sys.argv[1] if len(sys.argv) > 1 else "data/enriched/merged.csv"
    out = sys.argv[2] if len(sys.argv) > 2 else "data/enriched/merged_with_main.csv"
    main(inp, out)
