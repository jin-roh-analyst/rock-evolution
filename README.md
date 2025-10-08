🎸 The Evolution of Rock Music Popularity (1958 – Present)
📖 Overview

This project explores how rock music popularity has evolved globally — from its early dominance on Billboard charts to its position in the modern streaming era.
It combines historical Billboard Hot 100 data (1958 → today) with Spotify streaming and genre data to uncover long-term trends, regional differences, and subgenre shifts.

🧱 Project Structure
rock-evolution/
│
├── data/
│   ├── raw/         ← raw CSVs (Billboard, Spotify, World Bank)
│   ├── cleaned/     ← SQL-cleaned data
│   ├── enriched/    ← merged + genre-labeled data
│   └── final/       ← BI-ready datasets
│
├── src/             ← Python source scripts
│   ├── fetch_billboard_all_weeks.py      # pulls Billboard Hot 100 weekly charts
│   ├── fetch_billboard_week.py           # single-week fetcher (for testing)
│   ├── spotify_auth.py                   # Spotify API authentication
│   ├── enrich_spotify_genres.py          # adds artist-genre metadata
│   ├── musicbrainz_utils.py              # (optional) historical genre enrichment
│   └── hypothesis_testing.py             # Billboard vs Spotify correlation tests
│
├── sql/             ← SQL cleaning and normalization scripts
├── notebooks/       ← Jupyter notebooks for analysis & visualization
├── reports/         ← figures, dashboards, Medium drafts
└── README.md

🚀 Data Sources
Source	Years	Content	Access
Billboard Hot 100	1958 – present	Weekly chart ranks	billboard.py (unofficial API)
Spotify Charts	2013 – present	Weekly top 200 streams by country	spotifycharts.com

Spotify Web API	2013 – present	Track & artist metadata, genre tags	Official API
MusicBrainz / Discogs API	1950s – present	Historical genre classification	Public APIs
World Bank Open Data	1960 – present	Population, GDP, Internet penetration	data.worldbank.org
🧮 Tech Stack

Python 3.10 + → ETL & API access

billboard.py, requests, pandas, python-dotenv, duckdb

SQL / DuckDB → data cleaning & joins

Jupyter Notebook → exploratory analysis & visualization

Tableau / Power BI / Plotly → interactive dashboards

Medium + GitHub + LinkedIn → storytelling & portfolio sharing

📊 Project Phases

Data Collection

Scrape all Billboard Hot 100 weeks (1958 → today).

Gather Spotify Charts (2013 → present).

Pull artist genres from Spotify API.

Merge with World Bank population data.

Data Cleaning / Normalization (SQL)

Standardize columns, handle duplicates, convert dates.

Compute normalized popularity scores & streams per capita.

Analytics (Python)

Trend analysis: rock share vs pop & hip-hop.

Subgenre growth (alt rock, indie rock, punk rock, hard rock).

Statistical tests:

Spearman rank correlation (Billboard vs Spotify consistency)

Kolmogorov–Smirnov distribution test (popularity distributions)

Visualization / BI Dashboard

🌍 Map: Rock streams per capita by country

📈 Line chart: Rock share 1980 – 2025

🎵 Stacked area: Subgenre evolution

🔍 Scatter: Billboard vs Spotify rank correlation

Storytelling & Delivery

Medium article: “Is Rock Still Alive? A Global Data Journey.”

LinkedIn teaser post + dashboard preview.

GitHub repository with clean code and datasets.

🧠 Key Questions

How has rock’s share of music popularity changed since the 1960s?

Do Billboard ranks correlate with Spotify streams in the modern era?

Which countries still lead in rock listenership today?

Which subgenres (alt, indie, punk, hard rock) are growing or fading?

How do economic and demographic factors relate to rock popularity?

⚙️ Setup Instructions

Clone the repo

git clone https://github.com/<your-username>/rock-evolution.git
cd rock-evolution


Create environment

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


Add your Spotify API keys
Create .env in project root:

SPOTIFY_CLIENT_ID=your_id
SPOTIFY_CLIENT_SECRET=your_secret


Fetch data

python src/fetch_billboard_all_weeks.py
python src/enrich_spotify_genres.py


Explore

jupyter notebook

💡 Example Insight Preview

Between 1985 and 1995, rock dominated ≈ 60 % of Billboard Hot 100 entries.
By 2023, its share in Spotify’s global top 200 was under 15 %, with Latin America showing a renewed growth in indie and punk subgenres.

📈 Future Work

Predict 2030 rock share using time-series forecasting (Prophet).

Add concert/festival data for temporal correlation spikes.

Integrate YouTube Music and Apple Music metrics for cross-validation.

Deploy dashboard on Streamlit or Tableau Public.

👤 Author

Jinwoo Roh
🎓 UCLA Anderson MSBA Class of 2026
💼 Marketing & Data Analytics Professional
