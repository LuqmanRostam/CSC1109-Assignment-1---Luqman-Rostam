times = LOAD '/user/hive/data/university_rankings_clean/times_clean'
        USING PigStorage(',')
        AS (world_rank:int, university_name:chararray, country:chararray, total_score:double, year:int);

G = GROUP times BY (year, country);
A = FOREACH G GENERATE group.year AS year, group.country AS country, AVG(times.total_score) AS avg_score;
O = ORDER A BY year ASC, avg_score DESC;

STORE O INTO '/user/hive/out/pig_avg_country_year_score' USING PigStorage(',');




