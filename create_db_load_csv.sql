-- 1. create database
CREATE DATABASE OUR_FIRST_DATABASE;

-- 2. create tables
CREATE OR REPLACE TABLE history (
Crime_ID STRING,
Month STRING,
Reported_by STRING,
Falls_within STRING,
Longitude STRING,
Latitude STRING,
Location STRING,
LSOA_code STRING,
LSOA_name STRING,
Crime_type STRING,
Last_outcome_category STRING,
Context STRING
);
CREATE OR REPLACE TABLE analytics (
month string,
longitud string,
latitude string,
crime_type string,
number_crimes string
);
-- 3. Configure gcs bucket :https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html#step-1-create-a-cloud-storage-integration-in-snowflake
CREATE STORAGE INTEGRATION GCP_BUCKETS
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://landing_bucket_crime/')

CREATE STORAGE INTEGRATION GCP_BUCKETS_2
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://landing_hasting_sql_practice/')
-- Configure the landing bucket, Give the custom role to the principal That I get from snowflake: lvqwnudpvz@gcpeuropewest2-1-4e2d.iam.gserviceaccount.com
DESC STORAGE INTEGRATION GCP_BUCKETS;
DESC STORAGE INTEGRATION GCP_BUCKETS_2;
-- 4. make snowflake aware of the existence of the bucket
create or replace stage my_s3_stage url='gs://landing_bucket_crime/';


create or replace stage my_gcs_stage
url = 'gcs://landing_bucket_crime/'
storage_integration = GCP_BUCKETS;
file_format = my_csv_format;

create or replace stage my_gcs_stage_2 
url='gcs://landing_hasting_sql_practice/'
storage_integration = GCP_BUCKETS_2;

list @my_gcs_stage;

-- 5.copy at the beginning of each month from gcs to crime table and insert into analytics the desired data: number of crimes per location per type
copy into CRIME
  from @my_gcs_stage
  file_format = (type = csv field_delimiter = ',' skip_header = 1 error_on_column_count_mismatch=false );

-- Does not work, it works when I use  @my_gcs_stage
copy into TEST_CRIME
  from gs://landing_bucket_crime/2021-10-avon-and-somerset-street.csv
  pattern='*.csv'
  file_format = (type = csv field_delimiter = '|' skip_header = 1);


-- Does not recognize pattern
copy into TEST_CRIME
  from @my_gcs_stage;
  --  pattern='.*sales.*.csv';

copy into claims
  from @my_gcs_stage_2
  file_format = (type = csv field_delimiter = ',' skip_header = 1  DATE_FORMAT='DD-MM-YYYY' error_on_column_count_mismatch=false );
 
SELECT * FROM test_crime;

SELECT COUNT(*) FROM test_crime;

delete FROM claims;
DROP TABLE claims_2
ALTER TABLE CLAIMS_3 RENAME to claims


-- 6. check the last month was uploaded
select month,
      count(*)
from analytics
group by month

-- 7. create the table analytics
insert into analytics
select month, longitude, latitude, crime_type,
       count(*) as number_crimes
from crime
Group by month,longitude, latitude, crime_type
Order by number_crimes desc


-- 8. optimize the method by only inserting the last month from history table
insert into analytics
select month, longitude, latitude, crime_type,
       count(*) as number_crimes
from test
where month in(
  select max(month) from test
)
Group by month,longitude, latitude, crime_type
Order by number_crimes desc



--9. (optional as you can )convert into date the month string 
ALTER TABLE TEST ADD COLUMN new_date_created DATE;  
alter session set DATE_INPUT_FORMAT = 'MM-YYYY'
UPDATE test 
SET new_date_created = to_date(month,'YYYY-MM');


list @my_gcs_stage;

--partition by does not change the number of rows returned
select min(reported_by),
  COUNT(crime_type) over(partition by reported_by) as new
from test
group by reported_by

alter table test_crime rename to crime




-- 7. Exploratory data analysis
-- check for duplicates in the claimnumber
SELECT claimnumber, COUNT(*)
FROM claims
GROUP BY claimnumber
HAVING COUNT(*) > 1
-- 453464
-- 4366563674
-- 43554356456

-- check for duplicates in the claimnumber with the same name and date
SELECT name, claimnumber, date, COUNT(*)
FROM claims
GROUP BY name, claimnumber, date
HAVING COUNT(*) > 1

-- Dr. Jacob 4366563674 2

-- Tim 43554356456 2

SELECT a.*
FROM claims a
JOIN (SELECT claimnumber, name, COUNT(*)
FROM claims 
GROUP BY claimnumber, name
HAVING count(*) > 1 ) b
ON a.name = b.name
AND a.claimnumber = b.claimnumber

-- 3. convert  numbers to date
-- that date serial numbers are the number of days since 01-JAN-1900, as such I suggest leveraging the 
-- dateadd( ) function adding the numeric value of days since 01-JAN-1900, and either adding a new field 
-- on your table or "wrapping it" 
-- add your new column
ALTER TABLE t1 ADD COLUMN new_date_created DATE;
 
--run a simple update - "days since" January 1, 1900
alter session set DATE_INPUT_FORMAT = 'DD-MM-YYYY'
UPDATE t1 
SET new_date_created = DATEADD(DAY, date_created, to_date('01-01-1900'));

--select names starting with wi, ilike case insensitive
select name
from claims
where name like 'Wi%'
-- names with the same number jacob, tim and pluto
SELECT a.*
FROM claims a
JOIN (SELECT claimnumber, name, COUNT(*)
FROM claims 
GROUP BY claimnumber, name
HAVING count(*) > 1 ) b
on a.claimnumber = b.claimnumber

-- find non ascii elements
select 
    *
from 
    claims
where 
    name = cast(name as varchar(16777216))



























alter table claims2
add date_number NUMBER;

UPDATE claims2
   SET date_number = CAST(date AS Number)


alter table claims2
add date_date DATE;

-- this 5 digit format is called msdate 
-- and it starts from 1 Jan 1900. Easiest way to convert it 
-- into standard date is, select convert (datetime, 44321) - 2. 

alter session set DATE_INPUT_FORMAT = 'DD-MM-YYYY'
ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'AUTO';
SELECT DATEADD(day, 76505, '2020-09-28 00:00:00.000')
SELECT CONVERT(DateTime, 76505 - 36163)
SELECT TO_TIMESTAMP('2020-09-28') 




