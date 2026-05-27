from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "web"
INPUT = ROOT / "data" / "final" / "master.csv"
OUT = WEB / "public" / "data"

GENRE_COLORS = {
    "Rock": "#f5b84b",
    "Pop": "#6fc7d9",
    "HipHop": "#d85c4a",
    "RnB": "#b889ff",
    "Country": "#8bcf73",
    "Electronic": "#5f8cff",
    "Latin": "#f07eb3",
    "Jazz": "#d6a06d",
    "Folk": "#b7c98d",
    "Gospel": "#c8b46a",
    "Reggae": "#55b86f",
    "Blues": "#668bd8",
    "Classical": "#b8b8c8",
    "Afrobeat": "#ff8a4c",
    "World": "#9fb1a8",
    "Unknown": "#525252",
}

ROCK_TAGS = {
    "Alternative Rock": ["alternative rock", "alt rock"],
    "Indie Rock": ["indie rock"],
    "Classic Rock": ["classic rock"],
    "Hard Rock": ["hard rock"],
    "Metal": ["metal", "heavy metal", "nu metal", "metalcore", "death metal", "black metal"],
    "Punk Rock": ["punk", "punk rock", "pop punk", "post-punk"],
    "Grunge Rock": ["grunge"],
    "Folk Rock": ["folk rock"],
    "Blues Rock": ["blues rock"],
    "Progressive Rock": ["progressive rock", "prog rock", "psychedelic rock", "psych rock"],
    "Soft Rock": ["soft rock", "adult standards"],
    "Emo / Post Rock": ["emo", "post-rock", "post rock", "shoegaze"],
}


def clean_artist(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1]
    return re.sub(r"\s+", " ", text)


def safe_float(value: float | int | None) -> float | None:
    if value is None or pd.isna(value) or math.isinf(float(value)):
        return None
    return round(float(value), 4)


def pct(value: float | int | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value) * 100, 2)


def write_json(name: str, payload: object) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def classify_era(year: int) -> str:
    if year <= 1964:
        return "First Wave"
    if year <= 1974:
        return "Expansion"
    if year <= 1991:
        return "Arena Peak"
    if year <= 2004:
        return "Alternative Aftershock"
    if year <= 2014:
        return "Digital Fragmentation"
    return "Streaming Era"


def era_bounds(era: str) -> tuple[int, int]:
    bounds = {
        "First Wave": (1958, 1964),
        "Expansion": (1965, 1974),
        "Arena Peak": (1975, 1991),
        "Alternative Aftershock": (1992, 2004),
        "Digital Fragmentation": (2005, 2014),
        "Streaming Era": (2015, 2025),
    }
    return bounds[era]


def detect_subgenre(genres_raw: object) -> str:
    text = "" if pd.isna(genres_raw) else str(genres_raw).lower()
    for label, keywords in ROCK_TAGS.items():
        if any(keyword in text for keyword in keywords):
            return label
    return "Other Rock"


def main() -> None:
    df = pd.read_csv(INPUT, parse_dates=["date"])
    df["artist"] = df["artist"].map(clean_artist)
    df["year"] = df["date"].dt.year
    df["genre_main"] = df["genre_main"].fillna("Unknown")
    df["chart_points"] = 101 - df["rank"]
    df["era"] = df["year"].map(classify_era)

    complete = df[df["genre_main"] != "Unknown"].copy()
    all_years = sorted(df["year"].unique())

    annual = (
        df.groupby("year")
        .agg(
            total_entries=("rank", "size"),
            total_points=("chart_points", "sum"),
            unique_artists=("artist", "nunique"),
            missing_genre_entries=("genre_main", lambda s: int((s == "Unknown").sum())),
        )
        .reset_index()
    )
    annual["missing_genre_rate"] = annual["missing_genre_entries"] / annual["total_entries"]

    genre_year = (
        complete.groupby(["year", "genre_main"])
        .agg(
            entries=("rank", "size"),
            chart_points=("chart_points", "sum"),
            unique_artists=("artist", "nunique"),
            avg_rank=("rank", "mean"),
        )
        .reset_index()
    )
    totals = genre_year.groupby("year")["chart_points"].sum().rename("known_points")
    genre_year = genre_year.join(totals, on="year")
    genre_year["point_share"] = genre_year["chart_points"] / genre_year["known_points"]

    annual_records = []
    for year in all_years:
        year_row = annual[annual["year"] == year].iloc[0]
        rows = genre_year[genre_year["year"] == year]
        shares = {r.genre_main: float(r.point_share) for r in rows.itertuples()}
        hhi = sum(v * v for v in shares.values())
        entropy = -sum(v * math.log(v) for v in shares.values() if v > 0)
        rock = rows[rows["genre_main"] == "Rock"]
        rock_entries = int(rock["entries"].iloc[0]) if len(rock) else 0
        rock_points = int(rock["chart_points"].iloc[0]) if len(rock) else 0
        rock_share = float(rock["point_share"].iloc[0]) if len(rock) else 0
        annual_records.append(
            {
                "year": int(year),
                "era": classify_era(int(year)),
                "totalEntries": int(year_row.total_entries),
                "knownGenreEntries": int(year_row.total_entries - year_row.missing_genre_entries),
                "missingGenreEntries": int(year_row.missing_genre_entries),
                "missingGenreRate": pct(year_row.missing_genre_rate),
                "uniqueArtists": int(year_row.unique_artists),
                "rockEntries": rock_entries,
                "rockChartPoints": rock_points,
                "rockShare": pct(rock_share),
                "genreHHI": safe_float(hhi),
                "genreEntropy": safe_float(entropy),
            }
        )

    genre_records = []
    for row in genre_year.itertuples():
        genre_records.append(
            {
                "year": int(row.year),
                "genre": row.genre_main,
                "entries": int(row.entries),
                "chartPoints": int(row.chart_points),
                "uniqueArtists": int(row.unique_artists),
                "avgRank": safe_float(row.avg_rank),
                "pointShare": pct(row.point_share),
            }
        )

    artist_year = (
        complete.groupby(["year", "artist", "genre_main"])
        .agg(entries=("rank", "size"), chart_points=("chart_points", "sum"), peak_rank=("rank", "min"))
        .reset_index()
    )
    all_artist_year = (
        complete.groupby("artist")
        .agg(
            first_year=("year", "min"),
            last_year=("year", "max"),
            active_years=("year", "nunique"),
            entries=("rank", "size"),
            chart_points=("chart_points", "sum"),
            peak_rank=("rank", "min"),
            primary_genre=("genre_main", lambda s: s.value_counts().index[0]),
        )
        .reset_index()
    )
    all_artist_year["span"] = all_artist_year["last_year"] - all_artist_year["first_year"] + 1
    top_artists = (
        all_artist_year.sort_values(["chart_points", "entries", "peak_rank"], ascending=[False, False, True])
        .head(250)
        .to_dict(orient="records")
    )
    for row in top_artists:
        for key in ["first_year", "last_year", "active_years", "entries", "chart_points", "peak_rank", "span"]:
            row[key] = int(row[key])

    yearly_top = []
    for (year, genre), rows in artist_year.groupby(["year", "genre_main"]):
        top = rows.sort_values(["chart_points", "entries", "peak_rank"], ascending=[False, False, True]).head(10)
        for rank, row in enumerate(top.itertuples(), start=1):
            yearly_top.append(
                {
                    "year": int(year),
                    "genre": genre,
                    "rank": rank,
                    "artist": row.artist,
                    "entries": int(row.entries),
                    "chartPoints": int(row.chart_points),
                    "peakRank": int(row.peak_rank),
                }
            )

    churn_records = []
    for label, subset in [("All Genres", complete), ("Rock", complete[complete["genre_main"] == "Rock"])]:
        previous: set[str] = set()
        for year in all_years:
            current = set(subset.loc[subset["year"] == year, "artist"])
            if not current:
                continue
            new = current - previous
            retained = current & previous
            churn_records.append(
                {
                    "year": int(year),
                    "scope": label,
                    "artists": len(current),
                    "newArtists": len(new),
                    "retainedArtists": len(retained),
                    "newArtistRate": pct(len(new) / len(current) if current else 0),
                }
            )
            previous = current

    rock_rows = complete[complete["genre_main"] == "Rock"].copy()
    rock_rows["subgenre"] = rock_rows["genres_raw"].map(detect_subgenre)
    rock_named_rows = rock_rows[rock_rows["subgenre"] != "Other Rock"].copy()
    subgenre = (
        rock_named_rows.groupby("subgenre")
        .agg(entries=("rank", "size"), chart_points=("chart_points", "sum"), artists=("artist", "nunique"))
        .reset_index()
        .sort_values("chart_points", ascending=False)
    )
    subgenre_records = []
    for row in subgenre.itertuples():
        examples = (
            rock_named_rows[rock_named_rows["subgenre"] == row.subgenre]
            .groupby("artist")["chart_points"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .index.tolist()
        )
        subgenre_records.append(
            {
                "subgenre": row.subgenre,
                "entries": int(row.entries),
                "chartPoints": int(row.chart_points),
                "artists": int(row.artists),
                "exampleArtists": examples,
            }
        )

    subgenre_period_records = []
    for era in ["First Wave", "Expansion", "Arena Peak", "Alternative Aftershock", "Digital Fragmentation", "Streaming Era"]:
        start, end = era_bounds(era)
        period_rows = rock_named_rows[(rock_named_rows["year"] >= start) & (rock_named_rows["year"] <= end)]
        grouped = (
            period_rows.groupby("subgenre")
            .agg(entries=("rank", "size"), chart_points=("chart_points", "sum"), artists=("artist", "nunique"))
            .reset_index()
            .sort_values("chart_points", ascending=False)
        )
        period_total = int(grouped["chart_points"].sum()) if len(grouped) else 0
        for row in grouped.itertuples():
            examples = (
                period_rows[period_rows["subgenre"] == row.subgenre]
                .groupby("artist")["chart_points"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .index.tolist()
            )
            subgenre_period_records.append(
                {
                    "period": era,
                    "startYear": start,
                    "endYear": end,
                    "subgenre": row.subgenre,
                    "entries": int(row.entries),
                    "chartPoints": int(row.chart_points),
                    "pointShare": pct(row.chart_points / period_total if period_total else 0),
                    "artists": int(row.artists),
                    "exampleArtists": examples,
                }
            )

    peak = max(annual_records, key=lambda r: r["rockShare"])
    latest_full_year = max(y for y in all_years if y < 2026)
    latest = next(r for r in annual_records if r["year"] == latest_full_year)
    top_rock = next((a for a in top_artists if a["primary_genre"] == "Rock"), None)
    total_entries = len(df)
    missing_entries = int((df["genre_main"] == "Unknown").sum())

    summary = {
        "dateRange": {"start": str(df["date"].min().date()), "end": str(df["date"].max().date())},
        "years": {"start": int(min(all_years)), "end": int(max(all_years)), "latestFullYear": int(latest_full_year)},
        "rows": int(total_entries),
        "artists": int(df["artist"].nunique()),
        "knownGenreEntries": int(total_entries - missing_entries),
        "missingGenreEntries": missing_entries,
        "missingGenreRate": pct(missing_entries / total_entries),
        "rockPeak": peak,
        "latest": latest,
        "rockDeclineSincePeakPctPoints": round(peak["rockShare"] - latest["rockShare"], 2),
        "topRockArtist": top_rock,
        "genres": [{"name": k, "color": v} for k, v in GENRE_COLORS.items()],
        "eras": [
            {"name": "First Wave", "start": 1958, "end": 1964},
            {"name": "Expansion", "start": 1965, "end": 1974},
            {"name": "Arena Peak", "start": 1975, "end": 1991},
            {"name": "Alternative Aftershock", "start": 1992, "end": 2004},
            {"name": "Digital Fragmentation", "start": 2005, "end": 2014},
            {"name": "Streaming Era", "start": 2015, "end": int(max(all_years))},
        ],
    }

    methodology = {
        "sources": [
            "Billboard Hot 100 weekly chart rows from 1958-08-09 to latest local file.",
            "Spotify and MusicBrainz genre tags already enriched in the project data.",
        ],
        "metrics": [
            "Chart points are calculated as 101 - rank, so a #1 song contributes 100 points and a #100 song contributes 1 point.",
            "Point share divides a genre's annual chart points by all known-genre chart points in that year.",
            "HHI is the sum of squared genre point shares; higher values mean the chart is more concentrated.",
            "Artist churn compares each year's charting artists with the previous year's charting artists.",
        ],
        "limitations": [
            "Genre labels are rule-based and depend on Spotify/MusicBrainz tags.",
            "Rows without genre_main are included in data-quality metrics but excluded from genre-share denominators.",
            "2025 may be partial depending on the latest raw Billboard file date.",
        ],
    }

    write_json("summary.json", summary)
    write_json("annual.json", annual_records)
    write_json("genre_year.json", genre_records)
    write_json("artists.json", {"topArtists": top_artists, "yearlyTop": yearly_top})
    write_json("churn.json", churn_records)
    write_json("subgenres.json", subgenre_records)
    write_json("subgenre_periods.json", subgenre_period_records)
    write_json("methodology.json", methodology)
    print(f"Wrote dashboard data to {OUT}")


if __name__ == "__main__":
    main()
