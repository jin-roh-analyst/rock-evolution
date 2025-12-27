DROP TABLE IF EXISTS rock.master;
CREATE TABLE rock.master AS
    SELECT b.artist,
           rank,
           date,
           last_chart_year,
           genres_raw,
           genre_main
FROM rock.billboard_hot_raw AS b
LEFT JOIN rock.merged_with_main AS m
ON LOWER(b.artist) = LOWER(m.artist);