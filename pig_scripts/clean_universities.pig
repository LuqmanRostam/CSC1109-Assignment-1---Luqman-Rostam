
cwur_raw = LOAD '/user/hive/data/university_rankings_raw/cwurData.csv'
           USING PigStorage(',')
           AS (world_rank:int,
               institution:chararray,
               country:chararray,
               score:double);


times_raw = LOAD '/user/hive/data/university_rankings_raw/timesData.csv'
            USING PigStorage(',')
            AS (world_rank:int,
                university_name:chararray,
                country:chararray,
                total_score:double,
                year:int);



cwur_clean  = FILTER cwur_raw  BY institution IS NOT NULL
                                   AND country IS NOT NULL
                                   AND score IS NOT NULL;

times_clean = FILTER times_raw BY university_name IS NOT NULL
                                   AND country IS NOT NULL
                                   AND total_score IS NOT NULL
                                   AND year IS NOT NULL;


STORE cwur_clean  INTO '/user/hive/data/university_rankings_clean/cwur_clean'
    USING PigStorage(',');
STORE times_clean INTO '/user/hive/data/university_rankings_clean/times_clean'
    USING PigStorage(',');

