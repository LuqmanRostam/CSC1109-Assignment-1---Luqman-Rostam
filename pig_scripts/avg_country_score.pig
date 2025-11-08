
cwur = LOAD '/user/hive/data/university_rankings_clean/cwur_clean'
       USING PigStorage(',')
       AS (world_rank:int, institution:chararray, country:chararray, score:double);

G = GROUP cwur BY country;
A = FOREACH G GENERATE group AS country, AVG(cwur.score) AS avg_score;
O = ORDER A BY avg_score DESC;
T = LIMIT O 10;

STORE T INTO '/user/hive/out/pig_avg_country_score' USING PigStorage(',');

