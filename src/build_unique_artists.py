import re
import pandas as pd
from pathlib import Path

IN = Path("data/raw/billboard_hot-100_1958-08-09_to_latest.csv")
OUT = Path("data/cleaned/unique_artists.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

def primary_artist(name: str) -> str:
    if not isinstance(name, str): return ""
    n = name.lower().strip()
    # keep the first billed artist; strip common joiners
    n = re.split(r"\s*(feat\.|featuring|with|x|&|,|and)\s*", n)[0]
    n = re.sub(r"\s+", " ", n).strip()
    return n

df = pd.read_csv(IN, usecols=["artist"])
df["artist_primary"] = df["artist"].apply(primary_artist)
artists = (df["artist_primary"]
           .dropna()
           .loc[lambda s: s.str.len() > 0]
           .drop_duplicates()
           .sort_values()
           .reset_index(drop=True))

artists.to_csv(OUT, index=False, header=["artist"])
print(f"Wrote {OUT} with {len(artists):,} unique primary artists")
