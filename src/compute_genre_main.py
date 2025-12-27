# src/compute_genre_main.py
import pandas as pd
from pathlib import Path

IN  = Path("data/enriched/artist_genres_raw.csv")   # from script A
OUT = Path("data/enriched/artist_genres_mapped.csv")

# Buckets and simple precedence, identical to our plan
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

def decide_bucket(genres_list):
    if not genres_list: 
        return None
    text = " ".join([g.lower() for g in genres_list if g])

    # early exits
    for special in SPECIAL_FIRST:
        if any(kw in text for kw in BUCKETS[special]):
            return special

    # christian X
    if any(x in text for x in ["christian","worship","ccm"]):
        if any(kw in text for kw in BUCKETS["Rock"]):   return "Rock"
        if any(kw in text for kw in BUCKETS["HipHop"]): return "HipHop"
        if any(kw in text for kw in BUCKETS["Pop"]):    return "Pop"
        return "Gospel"

    # indie nudges
    scores = {k:0 for k in BUCKETS}
    if "indie rock" in text: scores["Rock"] += 2
    if "indie pop"  in text: scores["Pop"]  += 2

    for b, kws in BUCKETS.items():
        for kw in kws:
            if kw in text:
                scores[b] += 1

    # Pop–Rock tie tweak
    if scores["Pop"] > 0 and scores["Rock"] > 0:
        if any(k in text for k in ["punk","metal","grunge","emo","hard rock","pop punk","punk rock"]):
            scores["Rock"] += 1
        elif "pop rock" in text:
            scores["Rock"] += 1
        else:
            scores["Pop"] += 1

    top = max(scores, key=lambda k: scores[k])
    return top if scores[top] > 0 else "Other"

def main():
    df = pd.read_csv(IN)
    # split semicolon list to python list
    raw_lists = df["genres_raw"].fillna("").apply(lambda s: [t.strip() for t in s.split(";") if t.strip()])
    df["genre_main"] = raw_lists.apply(decide_bucket)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df):,} rows")

if __name__ == "__main__":
    main()
