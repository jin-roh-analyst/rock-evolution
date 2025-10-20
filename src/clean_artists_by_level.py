import pandas as pd
import re

# --- Config ---
INPUT_CSV  = "data/cleaned/unique_artists_year.csv"
OUTPUT_CSV = "data/cleaned/unique_artists_levels.csv"

# --- Load ---
df = pd.read_csv(INPUT_CSV)

def clean_artist_levels(name: str):
    if not isinstance(name, str) or not name.strip():
        return ("", "", "")

    # ----- L1 -----
    a = name.lower()
    b = re.sub(r'\s*feat.*$', '', a)
    c = re.sub(r'w/.*', '', b)
    d = re.sub(r'/.*', '', c)   # remove feat and anything after
    e = re.sub(r'\(.*', '', d)   # remove everything starting from '('
    f = re.sub(r'\[.*', '', e)   # remove everything starting from '['
    g = re.sub(r'\{.*', '', f)   # remove everything starting from '{' 
    l1 = g.strip()

    # ----- L2 -----
    b2 = re.sub(r'\bx\b.*', '', l1)
    c2 = re.sub(r'\+.*', '', b2)
    d2 = re.sub(r'\&.*', '', c2)
    e2 = re.sub(r'\,.*', '', d2)
    f2 = re.sub(r'\band\b.*', '', e2)
    g2 = re.sub(r'\bwith\b.*', '', f2)
    h2 = re.sub(r'\bduet\b.*', '', g2)
    l2 = h2.strip()

    # ----- L3 -----
    a3 = re.sub(r'\$', 's', l2) 
    b3 = re.sub(r'[^\w\s]', '', a3)  # remove punctuation (POSIX style)
    c3 = re.sub(r'\s+', ' ', b3)         # collapse multiple spaces
    # 
    l3 = c3.strip()

    return (l1, l2, l3)

# Apply
df[['l1', 'l2', 'l3']] = df['artist'].apply(lambda x: pd.Series(clean_artist_levels(x)))

# Save
df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved {len(df)} rows with cleaned levels → {OUTPUT_CSV}")
