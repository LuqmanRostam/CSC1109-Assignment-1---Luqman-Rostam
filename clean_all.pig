-- 1) Load raw CSVs from HDFS
cwur_raw = LOAD '/user/hive/data/university_rankings_raw/cwurData.csv'
  USING PigStorage(',')
  AS (world_rank:int, institution:chararray, country:chararray, score:double);

times_raw = LOAD '/user/hive/data/university_rankings_raw/timesData.csv'
  USING PigStorage(',')
  AS (world_rank:int, university_name:chararray, country:chararray, total_score:double, year:int);

shanghai_raw = LOAD '/user/hive/data/university_rankings_raw/shanghaiData.csv'
  USING PigStorage(',')
  AS (world_rank:int, institution:chararray, country:chararray, total_score:double);

-- 2) Basic clean: drop rows with missing/blank key fields
cwur_keep = FILTER cwur_raw BY
  (institution IS NOT NULL) AND (TRIM(institution) != '') AND
  (country IS NOT NULL)     AND (TRIM(country) != '') AND
  (score IS NOT NULL);

times_keep = FILTER times_raw BY
  (university_name IS NOT NULL) AND (TRIM(university_name) != '') AND
  (country IS NOT NULL)         AND (TRIM(country) != '') AND
  (total_score IS NOT NULL)     AND (year IS NOT NULL);

shanghai_keep = FILTER shanghai_raw BY
  (institution IS NOT NULL) AND (TRIM(institution) != '') AND
  (country IS NOT NULL)     AND (TRIM(country) != '') AND
  (total_score IS NOT NULL);

-- 3) Country standardisation (extend mappings by adding more nested REPLACE)
-- Map 'United States of America', 'United States', 'U.S.A.' -> 'USA'
cwur_norm = FOREACH cwur_keep GENERATE
  world_rank,
  institution,
  REPLACE(REPLACE(REPLACE(TRIM(country),
                          'United States of America','USA'),
                          'United States','USA'),
                          'U.S.A.','USA') AS country,
  score;

times_norm = FOREACH times_keep GENERATE
  world_rank,
  university_name,
  REPLACE(REPLACE(REPLACE(TRIM(country),
                          'United States of America','USA'),
                          'United States','USA'),
                          'U.S.A.','USA') AS country,
  total_score,
  year;

shanghai_norm = FOREACH shanghai_keep GENERATE
  world_rank,
  institution,
  REPLACE(REPLACE(REPLACE(TRIM(country),
                          'United States of America','USA'),
                          'United States','USA'),
                          'U.S.A.','USA') AS country,
  total_score;

-- 4) De-duplicate
cwur_nodup = DISTINCT cwur_norm;
times_nodup = DISTINCT times_norm;
shanghai_nodup = DISTINCT shanghai_norm;

-- 5) Store cleaned outputs in HDFS (CSV)
STORE cwur_nodup     INTO '/user/hive/data/university_rankings_clean/cwur_clean'     USING PigStorage(',');
STORE times_nodup    INTO '/user/hive/data/university_rankings_clean/times_clean'    USING PigStorage(',');
STORE shanghai_nodup INTO '/user/hive/data/university_rankings_clean/shanghai_clean' USING PigStorage(',');

