-- One-time (if not already enabled)
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP TABLE IF EXISTS rock.artists_levels;

CREATE TABLE rock.artists_levels AS
WITH base AS (
  SELECT artist
  FROM rock.stg_unique_artists
  WHERE artist IS NOT NULL AND length(trim(artist)) > 0
),
-- L1: keep first billed artist, drop (feat/with/&/x/and/comma) tails, remove brackets, squeeze spaces
l1_cte AS (
  SELECT
    artist,
    trim(regexp_replace(
           regexp_replace(
             regexp_replace(artist, '\s*(feat\.|featuring|with|x|&|,| and )\s.*$', '', 'i'),
           '\s*[\(\[].*?[\)\]]', '', 'g'),
         '\s+', ' ', 'g')) AS l1
  FROM base
),
-- L2: normalized matching key (lower + unaccent + &→and + drop quotes + drop trailing dots in initials)
l2_cte AS (
  SELECT
    artist, l1,
    regexp_replace(
      regexp_replace(
        regexp_replace(
          unaccent(lower(l1)),
        '[’''"]', '', 'g'),
      '\s*&\s*', ' and ', 'g'),
    '\.(?=\b)', '', 'g') AS l2
  FROM l1_cte
),
-- L3: aggressive fallback (remove slashes, hyphens, and non-alnum except spaces)
l3_cte AS (
  SELECT
    artist, l1, l2,
    regexp_replace(
      regexp_replace(
        regexp_replace(l2, '/', ' ', 'g'),
      '-', ' ', 'g'),
    '[^a-z0-9\s]', ' ', 'g') AS l3_raw
  FROM l2_cte
),
l3_norm AS (
  SELECT artist, l1, l2,
         trim(regexp_replace(l3_raw, '\s+', ' ', 'g')) AS l3
  FROM l3_cte
)
SELECT DISTINCT artist, l1, l2, l3
FROM l3_norm
WHERE artist <> '';
