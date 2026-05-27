"use client";

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  Treemap,
  XAxis,
  YAxis
} from "recharts";
import summaryData from "../public/data/summary.json";
import annualData from "../public/data/annual.json";
import genreYearData from "../public/data/genre_year.json";
import artistsData from "../public/data/artists.json";
import churnData from "../public/data/churn.json";
import subgenresData from "../public/data/subgenres.json";
import subgenrePeriodsData from "../public/data/subgenre_periods.json";
import methodologyData from "../public/data/methodology.json";
import type {
  AnnualRecord,
  ArtistsPayload,
  ChurnRecord,
  GenreName,
  GenreYearRecord,
  Methodology,
  SubgenreRecord,
  SubgenrePeriodRecord,
  Summary,
  YearlyTopArtist
} from "@/lib/types";

const summary = summaryData as Summary;
const annual = annualData as AnnualRecord[];
const genreYear = genreYearData as GenreYearRecord[];
const artists = artistsData as ArtistsPayload;
const churn = churnData as ChurnRecord[];
const subgenres = subgenresData as SubgenreRecord[];
const subgenrePeriods = subgenrePeriodsData as SubgenrePeriodRecord[];
const methodology = methodologyData as Methodology;

const priorityGenres: GenreName[] = ["Rock", "Pop", "HipHop", "RnB", "Country", "Electronic", "Latin", "Jazz"];
const genreColors = Object.fromEntries(summary.genres.map((g) => [g.name, g.color])) as Record<GenreName, string>;

function fmt(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

function pct(value: number | null | undefined) {
  if (value == null) return "n/a";
  return `${value.toFixed(1)}%`;
}

function StatCard({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      <div className="stat-note">{note}</div>
    </div>
  );
}

function SectionIntro({ kicker, title, children }: { kicker: string; title: string; children: React.ReactNode }) {
  return (
    <div className="section-intro">
      <span>{kicker}</span>
      <h2>{title}</h2>
      <p>{children}</p>
    </div>
  );
}

function ChartTooltip({ active, payload, label }: { active?: boolean; payload?: any[]; label?: string | number }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <strong>{label}</strong>
      {payload.map((item) => (
        <div key={`${item.name}-${item.value}`} style={{ color: item.color }}>
          {item.name}: {typeof item.value === "number" ? item.value.toLocaleString() : item.value}
        </div>
      ))}
    </div>
  );
}

function LongArcTooltip({ active, payload }: { active?: boolean; payload?: any[] }) {
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload as AnnualRecord | undefined;
  if (!row) return null;
  return (
    <div className="chart-tooltip">
      <strong>{row.year} · {row.era}</strong>
      <div style={{ color: "#f5b84b" }}>Rock share: {pct(row.rockShare)}</div>
      <div>Rock entries: {fmt(row.rockEntries)}</div>
      <div>Known genre entries: {fmt(row.knownGenreEntries)}</div>
      <div style={{ color: "#8f8a83" }}>Missing genre rate: {pct(row.missingGenreRate)}</div>
    </div>
  );
}

function TreemapNode(props: any) {
  const { x, y, width, height, name, fill } = props;
  if (width < 55 || height < 34) {
    return <rect x={x} y={y} width={width} height={height} fill={fill} stroke="#18120d" strokeWidth={2} />;
  }
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={fill} stroke="#18120d" strokeWidth={2} />
      <text x={x + 10} y={y + 20} className="treemap-label">
        {name}
      </text>
    </g>
  );
}

export default function Home() {
  const [selectedYear, setSelectedYear] = useState(summary.years.latestFullYear);
  const [artistQuery, setArtistQuery] = useState("");
  const [selectedGenre, setSelectedGenre] = useState<GenreName>("Rock");
  const [metric, setMetric] = useState<"pointShare" | "entries">("pointShare");
  const [selectedPeriod, setSelectedPeriod] = useState(summary.eras[2]?.name ?? "Arena Peak");

  const selectedAnnual = annual.find((row) => row.year === selectedYear) ?? summary.latest;
  const selectedTop = useMemo(() => {
    return artists.yearlyTop.filter((row) => row.year === selectedYear && row.genre === selectedGenre).slice(0, 10);
  }, [selectedGenre, selectedYear]);

  const filteredArtists = useMemo(() => {
    const q = artistQuery.trim().toLowerCase();
    return artists.topArtists
      .filter((artist) => (q ? artist.artist.toLowerCase().includes(q) : true))
      .slice(0, q ? 30 : 12);
  }, [artistQuery]);

  const genreShareRows = useMemo(() => {
    const byYear = new Map<number, Record<string, number>>();
    for (const row of genreYear) {
      if (!priorityGenres.includes(row.genre)) continue;
      const target = byYear.get(row.year) ?? { year: row.year };
      target[row.genre] = metric === "pointShare" ? row.pointShare : row.entries;
      byYear.set(row.year, target);
    }
    return Array.from(byYear.values()).sort((a, b) => Number(a.year) - Number(b.year));
  }, [metric]);

  const selectedGenreTrend = useMemo(() => {
    return genreYear
      .filter((row) => row.genre === selectedGenre)
      .map((row) => ({
        year: row.year,
        share: row.pointShare,
        entries: row.entries,
        chartPoints: row.chartPoints
      }));
  }, [selectedGenre]);

  const churnRows = churn.filter((row) => row.scope === selectedGenre || (selectedGenre !== "Rock" && row.scope === "All Genres"));
  const periodSubgenres = useMemo(() => {
    return subgenrePeriods.filter((row) => row.period === selectedPeriod);
  }, [selectedPeriod]);
  const dominantSubgenre = periodSubgenres[0];
  const treemapData = periodSubgenres.map((row, index) => ({
    name: row.subgenre,
    size: row.chartPoints,
    entries: row.entries,
    artists: row.artists,
    pointShare: row.pointShare,
    examples: row.exampleArtists.join(", "),
    fill: ["#f7c15a", "#d35f37", "#a9422b", "#f08b36", "#c7933c", "#e0d2a7", "#8f6a42"][index % 7]
  }));

  return (
    <main>
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Billboard Hot 100 · 1958-2025</p>
          <h1>Rock Evolution</h1>
          <p className="hero-text">
            A rank-weighted story of rock&apos;s rise, arena peak, fragmentation, and modern niche in the Hot 100.
          </p>
        </div>
        <div className="hero-panel hero-album">
          <div className="record-mark" />
          <span>Primary lens</span>
          <strong>Rank-weighted chart points</strong>
          <p>#1 songs carry more signal than #100 entries, so dominance is measured by impact, not just appearances.</p>
        </div>
      </section>

      <section className="stats-grid">
        <StatCard
          label="Rock Peak"
          value={`${summary.rockPeak.year} · ${pct(summary.rockPeak.rockShare)}`}
          note="Rank-weighted share of known-genre Hot 100 chart points."
        />
        <StatCard
          label="Latest Share"
          value={`${summary.latest.year} · ${pct(summary.latest.rockShare)}`}
          note={`${summary.rockDeclineSincePeakPctPoints.toFixed(1)} percentage points below the peak.`}
        />
        <StatCard
          label="Artists Tracked"
          value={fmt(summary.artists)}
          note={`${fmt(summary.rows)} weekly chart rows analyzed.`}
        />
        <StatCard
          label="Genre Gap"
          value={pct(summary.missingGenreRate)}
          note="Rows without a usable main genre are tracked as data quality risk."
        />
      </section>

      <section className="dashboard-section">
        <SectionIntro kicker="01 · The Long Arc" title="Rock did not fade evenly. It peaked, fractured, then became cyclical.">
          Chart points weight higher-ranked songs more heavily, making this trend less naive than raw entry counts.
        </SectionIntro>
        <div className="chart-card large">
          <ResponsiveContainer width="100%" height={380}>
            <ComposedChart data={annual} margin={{ top: 20, right: 28, bottom: 8, left: 0 }}>
              <defs>
                <linearGradient id="rockFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#f5b84b" stopOpacity={0.55} />
                  <stop offset="95%" stopColor="#f5b84b" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#2b2721" vertical={false} />
              <XAxis dataKey="year" stroke="#b9ac9b" tickLine={false} />
              <YAxis stroke="#b9ac9b" tickLine={false} tickFormatter={(v) => `${v}%`} />
              <Tooltip content={<LongArcTooltip />} />
              <Area type="monotone" dataKey="rockShare" name="Rock share" stroke="#f5b84b" fill="url(#rockFill)" strokeWidth={3} />
              <Line type="monotone" dataKey="missingGenreRate" name="Missing genre rate" stroke="#777" dot={false} strokeDasharray="5 5" />
            </ComposedChart>
          </ResponsiveContainer>
          <div className="insight-strip">
            <div>
              <strong>{summary.rockPeak.year}</strong>
              <span>peak rock year</span>
            </div>
            <div>
              <strong>{pct(summary.rockPeak.rockShare)}</strong>
              <span>peak rock share</span>
            </div>
            <div>
              <strong>{pct(summary.latest.rockShare)}</strong>
              <span>{summary.latest.year} rock share</span>
            </div>
            <div>
              <strong>{summary.rockDeclineSincePeakPctPoints.toFixed(1)} pts</strong>
              <span>decline since peak</span>
            </div>
          </div>
        </div>
      </section>

      <section className="dashboard-section two-column">
        <div>
          <SectionIntro kicker="02 · Genre Wars" title="The question is not only whether rock declined. It is who absorbed the chart space.">
            Switch between rank-weighted share and raw entries. The rank-weighted view gives #1 hits more influence.
          </SectionIntro>
          <div className="control-row">
            <button className={metric === "pointShare" ? "active" : ""} onClick={() => setMetric("pointShare")}>
              Chart-point share
            </button>
            <button className={metric === "entries" ? "active" : ""} onClick={() => setMetric("entries")}>
              Entry count
            </button>
          </div>
        </div>
        <div className="chart-card">
          <ResponsiveContainer width="100%" height={340}>
            <AreaChart data={genreShareRows} margin={{ top: 16, right: 20, bottom: 0, left: 0 }}>
              <CartesianGrid stroke="#2b2721" vertical={false} />
              <XAxis dataKey="year" stroke="#b9ac9b" tickLine={false} />
              <YAxis stroke="#b9ac9b" tickLine={false} />
              <Tooltip content={<ChartTooltip />} />
              <Legend />
              {priorityGenres.map((genre) => (
                <Area
                  key={genre}
                  type="monotone"
                  dataKey={genre}
                  stackId={metric === "pointShare" ? "share" : undefined}
                  stroke={genreColors[genre]}
                  fill={genreColors[genre]}
                  fillOpacity={0.62}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="dashboard-section two-column reverse">
        <div className="chart-card">
          <ResponsiveContainer width="100%" height={330}>
            <LineChart data={annual} margin={{ top: 14, right: 24, bottom: 0, left: 0 }}>
              <CartesianGrid stroke="#2b2721" vertical={false} />
              <XAxis dataKey="year" stroke="#b9ac9b" tickLine={false} />
              <YAxis stroke="#b9ac9b" tickLine={false} />
              <Tooltip content={<ChartTooltip />} />
              <Line type="monotone" dataKey="genreHHI" name="Genre concentration" stroke="#f5b84b" strokeWidth={3} dot={false} />
              <Line type="monotone" dataKey="genreEntropy" name="Genre fragmentation" stroke="#6fc7d9" strokeWidth={3} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <SectionIntro kicker="03 · Fragmentation" title="Rock's decline coincides with a less concentrated chart, not a one-for-one handoff.">
          HHI rises when fewer genres dominate. Entropy rises when chart points spread across more genres.
        </SectionIntro>
      </section>

      <section className="dashboard-section">
        <SectionIntro kicker="04 · Artist Motion" title="A genre can lose dominance before it loses artists. Churn shows the market rhythm.">
          New and retained artist counts reveal whether the chart is recycling familiar names or constantly refreshing.
        </SectionIntro>
        <div className="genre-tabs">
          {(["Rock", "Pop", "HipHop", "RnB", "Country"] as GenreName[]).map((genre) => (
            <button
              key={genre}
              className={selectedGenre === genre ? "active" : ""}
              style={{ borderColor: genreColors[genre] }}
              onClick={() => setSelectedGenre(genre)}
            >
              {genre}
            </button>
          ))}
        </div>
        <div className="split-grid">
          <div className="chart-card">
            <ResponsiveContainer width="100%" height={330}>
              <BarChart data={churnRows} margin={{ top: 14, right: 18, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="#2b2721" vertical={false} />
                <XAxis dataKey="year" stroke="#b9ac9b" tickLine={false} />
                <YAxis stroke="#b9ac9b" tickLine={false} />
                <Tooltip content={<ChartTooltip />} />
                <Bar dataKey="newArtists" name="New artists" fill="#f5b84b" />
                <Bar dataKey="retainedArtists" name="Retained artists" fill="#7c6d5e" />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="chart-card">
            <ResponsiveContainer width="100%" height={330}>
              <LineChart data={selectedGenreTrend} margin={{ top: 14, right: 20, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="#2b2721" vertical={false} />
                <XAxis dataKey="year" stroke="#b9ac9b" tickLine={false} />
                <YAxis stroke="#b9ac9b" tickLine={false} tickFormatter={(v) => `${v}%`} />
                <Tooltip content={<ChartTooltip />} />
                <Line type="monotone" dataKey="share" name={`${selectedGenre} share`} stroke={genreColors[selectedGenre]} strokeWidth={3} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </section>

      <section className="dashboard-section two-column">
        <SectionIntro kicker="05 · Rock Family Tree" title="Rock fragmented into families that peak differently and carry different artist stories.">
          Select a milestone era to see which rock subgenres dominated that period. Uncategorized `Other Rock` records are omitted from this view.
        </SectionIntro>
        <div className="chart-card family-card">
          <div className="period-tabs">
            {summary.eras.map((era) => (
              <button key={era.name} className={selectedPeriod === era.name ? "active" : ""} onClick={() => setSelectedPeriod(era.name)}>
                <strong>{era.name}</strong>
                <span>{era.start}-{era.end}</span>
              </button>
            ))}
          </div>
          {dominantSubgenre ? (
            <div className="period-summary">
              <div>
                <span>Dominant subgenre</span>
                <strong>{dominantSubgenre.subgenre}</strong>
              </div>
              <div>
                <span>Period share</span>
                <strong>{pct(dominantSubgenre.pointShare)}</strong>
              </div>
              <div>
                <span>Artists</span>
                <strong>{fmt(dominantSubgenre.artists)}</strong>
              </div>
              <p>{dominantSubgenre.exampleArtists.join(", ")}</p>
            </div>
          ) : null}
          <ResponsiveContainer width="100%" height={380}>
            <Treemap data={treemapData} dataKey="size" nameKey="name" content={<TreemapNode />} />
          </ResponsiveContainer>
        </div>
      </section>

      <section className="dashboard-section artist-section">
        <SectionIntro kicker="06 · Artist Hall" title="The strongest portfolio story is a dashboard that lets people argue with the rankings.">
          Search all-time artist rankings or inspect the top artists for the selected year and genre.
        </SectionIntro>
        <div className="artist-year-control">
          <div>
            <span>Selected year</span>
            <strong>{selectedYear}</strong>
          </div>
          <input
            aria-label="Selected year for Artist Hall"
            min={summary.years.start}
            max={summary.years.latestFullYear}
            value={selectedYear}
            onChange={(event) => setSelectedYear(Number(event.target.value))}
            type="range"
          />
        </div>
        <div className="artist-grid">
          <div className="table-card">
            <div className="table-head">
              <h3>All-Time Artist Rankings</h3>
              <input
                value={artistQuery}
                onChange={(event) => setArtistQuery(event.target.value)}
                placeholder="Search artist..."
                aria-label="Search artist"
              />
            </div>
            <div className="artist-list">
              {filteredArtists.map((artist, index) => (
                <div className="artist-row" key={`${artist.artist}-${artist.chart_points}`}>
                  <span>{index + 1}</span>
                  <strong>{artist.artist}</strong>
                  <em>{artist.primary_genre}</em>
                  <b>{fmt(artist.chart_points)} pts</b>
                </div>
              ))}
            </div>
          </div>
          <div className="table-card">
            <div className="table-head">
              <h3>
                Top {selectedGenre} Artists · {selectedYear}
              </h3>
            </div>
            <div className="artist-list">
              {selectedTop.map((artist: YearlyTopArtist) => (
                <div className="artist-row" key={`${artist.year}-${artist.genre}-${artist.artist}`}>
                  <span>{artist.rank}</span>
                  <strong>{artist.artist}</strong>
                  <em>Peak #{artist.peakRank}</em>
                  <b>{fmt(artist.chartPoints)} pts</b>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="methodology">
        <SectionIntro kicker="Methodology" title="The dashboard is opinionated, but the caveats stay visible.">
          This is a cultural analytics project, not an official genre taxonomy.
        </SectionIntro>
        <div className="method-grid">
          <div>
            <h3>Sources</h3>
            {methodology.sources.map((item) => (
              <p key={item}>{item}</p>
            ))}
          </div>
          <div>
            <h3>Metrics</h3>
            {methodology.metrics.map((item) => (
              <p key={item}>{item}</p>
            ))}
          </div>
          <div>
            <h3>Limitations</h3>
            {methodology.limitations.map((item) => (
              <p key={item}>{item}</p>
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}
