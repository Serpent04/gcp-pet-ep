DROP TABLE IF EXISTS anime_csv;

CREATE EXTERNAL TABLE anime_csv (
  MAL_ID INT,
  Name STRING,
  Score FLOAT,
  Genres STRING,
  english_name STRING,
  japanese_name STRING,
  Type STRING,
  Episodes INT,
  Aired STRING,
  Premiered STRING,
  Producers STRING,
  Licensors STRING,
  Studios STRING,
  Sources STRING,
  Duration STRING,
  Rating STRING,
  Ranked INT,
  Popularity INT,
  Members INT,
  Favorites INT,
  Watching INT,
  Completed INT,
  on_hold INT,
  Dropped INT,
  plan_to_watch INT,
  score_10 INT,
  score_9 INT,
  score_8 INT,
  score_7 INT,
  score_6 INT,
  score_5 INT,
  score_4 INT,
  score_3 INT,
  score_2 INT,
  score_1 INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';' 
LOCATION 'gs://anime-data/raw/anime/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE anime_csv;

DROP TABLE IF EXISTS anime_parquet;

CREATE EXTERNAL TABLE anime_parquet (
  MAL_ID INT,
  Name STRING,
  Score FLOAT,
  Genres STRING,
  english_name STRING,
  japanese_name STRING,
  Type STRING,
  Episodes INT,
  Aired STRING,
  Premiered STRING,
  Producers STRING,
  Licensors STRING,
  Studios STRING,
  Sources STRING,
  Duration STRING,
  Rating STRING,
  Ranked INT,
  Popularity INT,
  Members INT,
  Favorites INT,
  Watching INT,
  Completed INT,
  on_hold INT,
  Dropped INT,
  plan_to_watch INT,
  score_10 INT,
  score_9 INT,
  score_8 INT,
  score_7 INT,
  score_6 INT,
  score_5 INT,
  score_4 INT,
  score_3 INT,
  score_2 INT,
  score_1 INT
)
STORED AS PARQUET 
LOCATION 'gs://anime-data/preprocessed/anime/';

INSERT OVERWRITE TABLE anime_parquet
SELECT *
FROM anime_csv;

DROP TABLE IF EXISTS anime_with_synopsis_csv;

CREATE EXTERNAL TABLE anime_with_synopsis_csv (
  MAL_ID INT,
  Name STRING,
  Score FLOAT,
  Genres STRING,
  Synopsis STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
LOCATION 'gs://anime-data/raw/anime_with_synopsis/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE anime_with_synopsis_csv;

DROP TABLE IF EXISTS anime_with_synopsis_parquet;

CREATE EXTERNAL TABLE anime_with_synopsis_parquet (
  MAL_ID INT,
  Name STRING,
  Score FLOAT,
  Genres STRING,
  Synopsis STRING
)
STORED AS PARQUET 
LOCATION 'gs://anime-data/preprocessed/anime_with_synopsis/';

INSERT OVERWRITE TABLE anime_with_synopsis_parquet
SELECT *
FROM anime_with_synopsis_csv;

DROP TABLE IF EXISTS animelist_csv;

CREATE EXTERNAL TABLE animelist_csv (
  user_id INT,
  anime_id INT,
  rating INT,
  watching_status INT,
  watched_episodes INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://anime-data/raw/animelist/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE animelist_csv;

DROP TABLE IF EXISTS animelist_parquet;

CREATE EXTERNAL TABLE animelist_parquet (
  user_id INT,
  anime_id INT,
  rating INT,
  watching_status INT,
  watched_episodes INT
)
STORED AS PARQUET 
LOCATION 'gs://anime-data/preprocessed/animelist/';

INSERT OVERWRITE TABLE animelist_parquet
SELECT *
FROM animelist_csv;

DROP TABLE IF EXISTS rating_complete_csv;

CREATE EXTERNAL TABLE rating_complete_csv (
  user_id INT,
  anime_id INT,
  rating INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://anime-data/raw/rating_complete/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE rating_complete_csv;

DROP TABLE IF EXISTS rating_complete_parquet;

CREATE EXTERNAL TABLE rating_complete_parquet (
  user_id INT,
  anime_id INT,
  rating INT
)
STORED AS PARQUET 
LOCATION 'gs://anime-data/preprocessed/rating_complete/';

INSERT OVERWRITE TABLE rating_complete_parquet
SELECT *
FROM rating_complete_csv;

DROP TABLE IF EXISTS watching_status_csv;

CREATE EXTERNAL TABLE watching_status_csv (
  status INT,
  description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'gs://anime-data/raw/watching_status/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE watching_status_csv;

DROP TABLE IF EXISTS watching_status_parquet;

CREATE EXTERNAL TABLE watching_status_parquet (
  status INT,
  description STRING
)
STORED AS PARQUET 
LOCATION 'gs://anime-data/preprocessed/watching_status/';

INSERT OVERWRITE TABLE watching_status_parquet
SELECT *
FROM watching_status_csv;