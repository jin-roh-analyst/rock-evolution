SELECT
  artist,
  EXTRACT(YEAR FROM MAX(date)) AS last_chart_year
FROM rock.billboard_hot_raw
GROUP BY artist
ORDER BY artist;
