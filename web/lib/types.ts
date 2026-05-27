export type GenreName =
  | "Rock"
  | "Pop"
  | "HipHop"
  | "RnB"
  | "Country"
  | "Electronic"
  | "Latin"
  | "Jazz"
  | "Folk"
  | "Gospel"
  | "Reggae"
  | "Blues"
  | "Classical"
  | "Afrobeat"
  | "World"
  | "Unknown";

export type Summary = {
  dateRange: { start: string; end: string };
  years: { start: number; end: number; latestFullYear: number };
  rows: number;
  artists: number;
  knownGenreEntries: number;
  missingGenreEntries: number;
  missingGenreRate: number;
  rockPeak: AnnualRecord;
  latest: AnnualRecord;
  rockDeclineSincePeakPctPoints: number;
  topRockArtist: ArtistRecord | null;
  genres: { name: GenreName; color: string }[];
  eras: { name: string; start: number; end: number }[];
};

export type AnnualRecord = {
  year: number;
  era: string;
  totalEntries: number;
  knownGenreEntries: number;
  missingGenreEntries: number;
  missingGenreRate: number;
  uniqueArtists: number;
  rockEntries: number;
  rockChartPoints: number;
  rockShare: number;
  genreHHI: number;
  genreEntropy: number;
};

export type GenreYearRecord = {
  year: number;
  genre: GenreName;
  entries: number;
  chartPoints: number;
  uniqueArtists: number;
  avgRank: number;
  pointShare: number;
};

export type ArtistRecord = {
  artist: string;
  first_year: number;
  last_year: number;
  active_years: number;
  entries: number;
  chart_points: number;
  peak_rank: number;
  primary_genre: GenreName;
  span: number;
};

export type YearlyTopArtist = {
  year: number;
  genre: GenreName;
  rank: number;
  artist: string;
  entries: number;
  chartPoints: number;
  peakRank: number;
};

export type ArtistsPayload = {
  topArtists: ArtistRecord[];
  yearlyTop: YearlyTopArtist[];
};

export type ChurnRecord = {
  year: number;
  scope: "All Genres" | "Rock";
  artists: number;
  newArtists: number;
  retainedArtists: number;
  newArtistRate: number;
};

export type SubgenreRecord = {
  subgenre: string;
  entries: number;
  chartPoints: number;
  artists: number;
  exampleArtists: string[];
};

export type SubgenrePeriodRecord = SubgenreRecord & {
  period: string;
  startYear: number;
  endYear: number;
  pointShare: number;
};

export type Methodology = {
  sources: string[];
  metrics: string[];
  limitations: string[];
};
