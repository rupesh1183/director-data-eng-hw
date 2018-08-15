--============Step 0 : Create external glue schema ========================================================================

create schema ratings location 's3://<bucket>/ratings/';

--============Step 1 : Create an external table that encompasses all date partitions containing daily ratings files========
--=========================================================================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ratings.player_subject_ratings(
ratings_ts timestamp,
player_id string, 
subject_id string,
ratings_type integer)
PARTITIONED BY (year int, month int, day date)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/player_subject_ratings/';

--============Step 2 : Create MANAGED table that only spans across the daily partition for the daily ratings file==========
--=========================================================================================================================

DROP TABLE IF EXISTS ratings.player_subject_ratings_{date_value};  
----==== Use above drop step if only one file is expected per day else do insert only into the daily partiton table.
--====== Dropping the table before creating allows for a re-runnable process, if multiple files are expected the same day -
--======= - a more complex process would be needed.

CREATE TABLE IF NOT EXISTS ratings.player_subject_ratings_{date_value}(   ---=== _{date_value} corresponds to daily partition
ratings_ts timestamp,
player_id string, 
subject_id string,
ratings_type integer)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/player_subject_ratings/year={year_value}/month={month_value}/day={date_value}/'; ---

--=== In the above CREATE TABLE step, the {date_value},{month_value} and {year_value} are to be passed as a parameter from the python/unix wrapper.

--============Step 3 : Create EXTERNAL table to hold ratings_type and its meaning (reference data)==========================
--===============For this step create a csv file and drop it in an s3 folder ==============================================

CREATE EXTERNAL TABLE IF NOT EXISTS ratings.ratings_type(
ratings_type integer,
meaning string)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/ratings_type/';   ---=====corresponds to the s3 location holding csv file for ratings_type meanings




--============Step 1 : Create an external table that encompasses all date partitions containing daily ratings files========
--=========================================================================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ratings.player_subject_interaction_snapshot(
ratings_ts timestamp,
ratings_date date,
player_id string, 
subject_id string,
ratings_type integer,
player_subject_interaction_id string,
unique_interaction_id string)
PARTITIONED BY (year int, month int, day date)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/player_subject_interaction_snapshot/';

--============Step 5 : Create MANAGED table to hold subject/player interactions===============================================
--===============Assign an interaction ID to tie subject/player and player/subject interactions ==============================

DROP TABLE IF EXISTS ratings.player_subject_interaction_snapshot_{date_value};
CREATE TABLE IF NOT EXISTS ratings.player_subject_interaction_snapshot_{date_value}(   ---=== _{date_value} corresponds to daily partition
ratings_ts timestamp,
ratings_date date,
player_id string, 
subject_id string,
ratings_type integer,
player_subject_interaction_id string,
unique_interaction_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/player_subject_interaction_snapshot/year={year_value}/month={month_value}/day={date_value}/';


--============Step 6 :  CREATE MOST CURRENT SNAPSHOT OF ALL PLAYER SUBJECT INTERCATIONS in the DAILY PARTITION================
--==================TIE THE INTERACTIONS TOGETHER WITH UNIQUE ID==============================================================
--==================THIS will allow for more complex rating_type associations such as remove vs block vs reject===============
INSERT INTO ratings.player_subject_interaction_snapshot_{date_value}
SELECT
t1.ratings_ts as ratings_ts,
cast(t1.ratings_ts as date) as ratings_date,
t1.player_id as player_id,
t1.subject_id as subject_id,
t1.ratings_type as ratings_type,
t1.player_id||t2.player_id as player_subject_interaction_id,   ------============player_subject_interaction
t1.player_id||t2.player_id as unique_interaction_id            ------============unique interaction_id
from
ratings.player_subject_ratings t1,     ---====Note this table sits on top of all daily partitions
ratings.player_subject_ratings t2      ---====Note this table sits on top of all daily partitions
where t1.player_id=t2.subject_id
and t1.subject_id=t2.player_id

UNION ALL

SELECT
t2.ratings_ts as ratings_ts,
cast(t2.ratings_ts as date) as ratings_date,
t2.player_id as player_id,
t2.subject_id as subject_id,
t2.ratings_type as ratings_type,
t2.player_id||t1.player_id as player_subject_interaction_id,   ------============player_subject_interaction
t1.player_id||t2.player_id as unique_interaction_id            ------============unique interaction_id

from
ratings.player_subject_ratings t1,    ---====Note this table sits on top of all daily partitions
ratings.player_subject_ratings t2     ---====Note this table sits on top of all daily partitions
where t1.player_id=t2.subject_id
and t1.subject_id=t2.player_id


--============Step 7 : Create external table metric ids and metric definitions======================================================
--==================================================================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS ratings.dim_metric(
metric_id integer,
metric_description string,
load_ts timestamp)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/dim_metric/'; 




--============Step 8 : Create an external table that encompasses all date partitions containing daily ratings metrics==============
--===============================================================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS ratings.daily_ratings_metrics(   ---=== _{date_value} corresponds to daily partition
ratings_date date,
ratings_year_month string
total_ratings integer,
total_skips integer,
total_likes integer,
total_reports integer,
total_rejects integer,
total_responded_interactions integer,
like_percentage float,
skip_percentage float,
reports_percentage float,
rejects_percentage float,
match_percentage float,
responded_interactions_percentage float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/daily_ratings_metrics/';


--============Step 9 : Create table to hold daily ratings metrics============================================================
--============================================================================================================================

DROP TABLE IF EXISTS ratings.daily_ratings_metrics_{date_value};
CREATE TABLE IF NOT EXISTS ratings.daily_ratings_metrics_{date_value}(   ---=== _{date_value} corresponds to daily partition
ratings_date date,
ratings_year_month string
total_ratings integer,
total_skips integer,
total_likes integer,
total_reports integer,
total_rejects integer,
total_responded_interactions integer,
like_percentage float,
skip_percentage float,
reports_percentage float,
rejects_percentage float,
match_percentage float,
responded_interactions_percentage float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/daily_ratings_metrics/year={year_value}/month={month_value}/day={date_value}/';



--============Step 10 : Create table to hold daily ratings metrics============================================================
--============================================================================================================================

INSERT INTO ratings.daily_ratings_metrics_{date_value}

SELECT 
ratings.ratings_date as ratings_date,
YEAR(ratings.ratings_date)||'-'||MONTH(ratings.ratings_date) as ratings_year_month,
ratings.count as total_ratings,
skips.count as total_skips,
likes.count as total_likes,
reports.count as total_reports,
rejects.count as total_rejects,
matches.count as total_matches,
response.count as total_responded_interactions,
(cast(likes.count as float)/cast(ratings.count as float))*100 as like_percentage,
(cast(skips.count as float)/cast(ratings.count as float))*100  as skip_percentage,
(cast(reports.count as float)/cast(ratings.count as float))*100  as reports_percentage,
(cast(rejects.count as float)/cast(ratings.count as float))*100  as rejects_percentage,
(cast(matches.count as float)/cast(ratings.count as float))*100  as match_percentage,
(cast(response.count as float)/cast(ratings.count as float))*100  as responded_interactions_percentage

FROM

(select count(*) as count ,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value} group by cast(ratings_ts as date)) ratings,

(select count(*) as count,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value}
where rating_type in (1,2) group by cast(ratings_ts as date)) likes,

(select count(*) as count,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value}
where rating_type = 0 group by cast(ratings_ts as date)) skips,

(select count(*) as count,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value}
where rating_type = 3 group by cast(ratings_ts as date)) rejects,

(select count(*) as count,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value}
where rating_type = 4 group by cast(ratings_ts as date)) reports,

(select count(*) as count,cast(ratings_ts as date) as ratings_date from 
ratings.player_subject_ratings_{date_value}
where rating_type = 5 group by cast(ratings_ts as date)) matches,

(
SELECT count(*) as count,cast(ratings_ts as date) as ratings)date
from
ratings.player_subject_interaction_snapshot_{date_value}
where ratings_type in (3,4,5) group by cast(ratings_ts as date)
) response

where ratings.ratings_date=likes.ratings_date
and ratings.ratings_date=skips.ratings_date
and ratings.ratings_date=skips.ratings_date
and ratings.ratings_date=rejects.ratings_date
and ratings.ratings_date=reports.ratings_date
and ratings.ratings_date=matches.ratings_date




--============Step 11 : INSERT metric ids and metric definitions=====================================================================
--==================================================================================================================================

INSERT INTO ratings.dim_metric
VALUES
(1,'average like rate',current_timestamp()),
(2,'average reciprocation rate',current_timestamp()),
(3,'average number of skips',current_timestamp()),
(4,'rate of change of reports',current_timestamp());


--=====Either above can be done in RDS to assign integer ids and then unloaded to S3 to or a REDIS based counter can be used
--=====in a distributed environment such as hive.



--============Step 12 : Create an external table that encompasses all date partitions containing monthly_interaction_metrics=======
--===============================================================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS ratings.monthly_interaction_metrics(   ---=== _{date_value} corresponds to daily partition
ratings_year_month string
metric_id integer,
metric_value float)
PARTITIONED BY (year int, month int, day date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/monthly_interaction_metrics/';


--============Step 13 : Create table to hold monthly_interaction_metrics============================================================
--================================================================================================================================

DROP TABLE IF EXISTS ratings.monthly_interaction_metrics_{date_value};
CREATE TABLE IF NOT EXISTS ratings.monthly_interaction_metrics_{date_value}(   ---=== _{date_value} corresponds to daily partition
ratings_year_month string
metric_id integer,
metric_value float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://<bucket>/ratings/monthly_interaction_metrics/year={year_value}/month={month_value}/day={date_value}/';


--============Step 14 : INSERT INTO monthly_interaction_metrics============================================================
--=================================================================================================================================

--Assumes that date dimension table exists and so average monthly metrics can be created by using number of days in a month
-- using date dimension and the daily metrics from ratings.daily_ratings_metrics_{date_value} having done a group by on ratings_year_month
--column