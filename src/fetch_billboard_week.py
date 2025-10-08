# Fetch ALL Billboard Hot 100 weeks from 1958-08-09 to latest
# Output appends safely and dedupes by chart week date.

import time
from pathlib import Path
from datetime import date, timedelta, datetime
import pandas as pd
import billboard  # pip install billboard.py

CHART = "hot-100"
START = date(1958, 8, 9)           # first Hot 100 week
END   = date.today()               # latest
STEP  = timedelta(days=7)          # weekly stride
OUT   = Path("data/raw/billboard_hot-100_1958-08-09_to_latest.csv")

SLEEP_SECONDS = 0.5  # be polite; increase if you see errors
FLUSH_EVERY   = 10   # flush to disk every N unique weeks

def load_existing_dates():
    if not OUT.exists():
        return set()
    try:
        df = pd.read_csv(OUT, usecols=["date"])
        return set(df["date"].astype(str).tolist())
    except Exception:
        return set()

def append_rows(rows):
    df = pd.DataFrame(rows, columns=["chart_name","date","rank","title","artist"])
    if OUT.exists():
        df.to_csv(OUT, mode="a", index=False, header=False)
    else:
        df.to_csv(OUT, index=False)

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    seen_dates = load_existing_dates()
    print(f"Already have {len(seen_dates)} weeks in {OUT.name}")

    cur = START
    buffer = []
    new_weeks = 0

    while cur <= END:
        qdate = cur.strftime("%Y-%m-%d")
        try:
            chart = billboard.ChartData(CHART, date=qdate)
        except Exception as e:
            print(f"[skip] {qdate}: {e}")
            cur += STEP
            time.sleep(SLEEP_SECONDS)
            continue

        week = chart.date  # Billboard’s canonical week string
        if not week:
            print(f"[no week] for query {qdate}")
            cur += STEP
            time.sleep(SLEEP_SECONDS)
            continue

        if week in seen_dates:
            # We already saved this exact chart week — skip
            print(f"[dup] {week}")
        else:
            print(f"[save] {week}")
            rows = [{"chart_name": CHART, "date": week,
                     "rank": e.rank, "title": e.title, "artist": e.artist}
                    for e in chart]
            buffer.extend(rows)
            seen_dates.add(week)
            new_weeks += 1

            if new_weeks % FLUSH_EVERY == 0:
                append_rows(buffer)
                buffer = []

        cur += STEP
        time.sleep(SLEEP_SECONDS)

    if buffer:
        append_rows(buffer)

    print(f"Done. Unique weeks total in file: {len(seen_dates)}")
    print(f"Wrote: {OUT.resolve()}")

if __name__ == "__main__":
    main()
