-- Cyclistic Bike-Share Data Analysis SQL Script
-- This script documents the end-to-end process for loading, combining, cleaning, and analyzing the Cyclistic bike-share dataset in MySQL. 
-- It follows the data analysis phases from the case study: Prepare (loading and combining), Process (cleaning), and Analyze (descriptive statistics and patterns). 
-- Notes from the original script are preserved in SQL comments (--). 
-- The script assumes the 12 monthly CSV files are available in the specified path.[Data for all the months(Dec 21 - Nov 22) was downloaded from [this source](https://divvy-tripdata.s3.amazonaws.com/index.html), stored in local storage,and then combined in MySQL Workbench.]
-- Total raw combined rows: 5,182,027 (with duplicates); after UNION (distinct): 4,818,877. After cleaning: 4,714,116 rows.

-- ## . Load Monthly Data into Individual Tables
-- Manually load each month's CSV into separate tables using `LOAD DATA LOCAL INFILE`. 
-- This ignores the header row (line 1) and handles comma-separated fields enclosed in quotes.

-- Example for February 2022 (repeat for jan22, march22, ..., december22)
CREATE TABLE feb22 (
    ride_id TEXT,                                                            -- ride_id should be unique. It should be the primary Key column
    rideable_type TEXT,
    started_at TEXT,
    ended_at TEXT,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat DOUBLE,
    start_lng DOUBLE,
    end_lat DOUBLE,
    end_lng DOUBLE,
    member_casual TEXT
);

LOAD DATA LOCAL INFILE "C:/Users/risha/Desktop/capstone project/Coursera/Google analytics/data from previous 12 months/feb22.csv"                      
INTO TABLE feb22
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;                                                                                            -- Doesn't include headers in infile because we ignored line 1.
  

-- ## . Combine Monthly Tables into a Single Dataset
-- Create a combined table and use `UNION` to merge all months, ensuring distinct rows (removes duplicates from raw 5,182,027 to 4,818,877).

CREATE TABLE combined_data (
    ride_id TEXT,
    rideable_type TEXT,
    started_at TEXT,
    ended_at TEXT,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat DOUBLE,
    start_lng DOUBLE,
    end_lat DOUBLE,
    end_lng DOUBLE,
    member_casual TEXT
);

INSERT INTO combined_data                 -- SELECT UNION as it returns distinct rows. (If used UNION ALL the combined raw data had 5182027) .  
SELECT * FROM jan22
UNION
SELECT * FROM feb22
UNION
SELECT * FROM march22
UNION
SELECT * FROM april22
UNION
SELECT * FROM may22
UNION
SELECT * FROM june22
UNION
SELECT * FROM july22
UNION
SELECT * FROM august22
UNION
SELECT * FROM september22
UNION
SELECT * FROM october22
UNION
SELECT * FROM november22
UNION
SELECT * FROM december22;                                       -- (Total count of raw combined dataset is 5182027). After union, only distinct values are used- rowcount is 4818877.

  

-- ## . Check for NULL Values
-- Verify data integrity: Total rows and NULL counts per column.

-- 1) Count total number of rows
SELECT COUNT(*) AS total_rows
FROM combined_data;
    -- '4818877'- total rows count. No duplicates

-- 2) NULL check column by column
SELECT
    SUM(CASE WHEN ride_id IS NULL THEN 1 ELSE 0 END) AS null_ride_id,
    SUM(CASE WHEN rideable_type IS NULL THEN 1 ELSE 0 END) AS null_rideable_type,
    SUM(CASE WHEN started_at IS NULL THEN 1 ELSE 0 END) AS null_started_at,
    SUM(CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END) AS null_ended_at,
    SUM(CASE WHEN start_station_name IS NULL THEN 1 ELSE 0 END) AS null_start_station_name,
    SUM(CASE WHEN start_station_id IS NULL THEN 1 ELSE 0 END) AS null_start_station_id,
    SUM(CASE WHEN end_station_name IS NULL THEN 1 ELSE 0 END) AS null_end_station_name,
    SUM(CASE WHEN end_station_id IS NULL THEN 1 ELSE 0 END) AS null_end_station_id,
    SUM(CASE WHEN start_lat IS NULL THEN 1 ELSE 0 END) AS null_start_lat,
    SUM(CASE WHEN start_lng IS NULL THEN 1 ELSE 0 END) AS null_start_lng,
    SUM(CASE WHEN end_lat IS NULL THEN 1 ELSE 0 END) AS null_end_lat,
    SUM(CASE WHEN end_lng IS NULL THEN 1 ELSE 0 END) AS null_end_lng,
    SUM(CASE WHEN member_casual IS NULL THEN 1 ELSE 0 END) AS null_member_casual
FROM combined_data;

-- ## . Check for Empty Strings
-- Identify rows with empty strings in key columns (1,100,271 rows affected).

-- 3) Check empty strings in key columns
SELECT COUNT(*) AS empty_rows
FROM combined_data
WHERE ride_id = ''
   OR rideable_type = ''
   OR started_at = '' OR ended_at = '' OR start_station_name = ''
   OR start_station_id = ''
   OR end_station_name = ''
   OR end_station_id = '' OR start_lat = '' OR end_lat = '' OR start_lng = '' OR end_lng = ''
   OR member_casual = '';
    -- 1100271 number of rows containing empty string
LIMIT 50;

-- 4) Sample rows with empty strings
SELECT ride_id, start_station_name, start_station_id,
       end_station_name, end_station_id, member_casual
FROM combined_data
WHERE start_station_name = ''
   OR start_station_id = ''
   OR end_station_name = ''
   OR end_station_id = ''
   OR member_casual = ''
LIMIT 50;
    -- We did find many columns containing empty strings - start & end station name and id, [start&end lat long are also empty - 5024 rows]
    -- Member_casual and started_at and ended_at - no empty strings

-- ## 6. Cleaning: Remove Empty Lat/Lng Rows
-- Remove 5,024 rows with empty coordinates (~0.1% of data).

-- 6) Empty Values in Latitude/Longitude Columns (start_lat, start_lng, end_lat, end_lng): 5024 Rows
-- 5024 rows is only ~0.1% of your 4.8M total, so removing them won't significantly reduce your dataset size.
-- Recommendation: Remove these rows to ensure data integrity, as empty coordinates could indicate incomplete trips.
DELETE FROM combined_data
WHERE start_lat = '' OR start_lng = '' OR end_lat = '' OR end_lng = '';

-- Documentation Note: "Removed 5024 rows with empty latitude/longitude values using DELETE query, as they represent <0.1% of data and are not essential for core analysis." -- now total count '4813853'

-- ## 7. Cleaning: Impute Empty Station Values
-- Impute ~1,100,271 empty station fields to 'Unknown' (~23% of data retained for ride patterns).

-- 7) Empty Values in Station Columns (1100271 number of rows containing empty string)
-- This is ~23% of your data. Removing them would lose a significant portion, potentially biasing trends (e.g., if missing more for casual riders). Since the case study doesn't require station-based analysis (focus is on ride patterns), retain the rows by imputing.
-- Impute empty strings to 'Unknown' to keep the data for ride length and user type metrics.
UPDATE combined_data
SET start_station_name = 'Unknown'
WHERE TRIM(start_station_name) = '';

UPDATE combined_data
SET start_station_id = 'Unknown'
WHERE TRIM(start_station_id) = '';

UPDATE combined_data
SET end_station_name = 'Unknown'
WHERE TRIM(end_station_name) = '';

UPDATE combined_data
SET end_station_id = 'Unknown'
WHERE TRIM(end_station_id) = '';

-- Documentation Note: "Imputed 1,100,271 rows with empty station names/IDs to 'Unknown' using TRIM() in WHERE clauses to handle whitespace variations, retaining ~23% of data as stations are not core to the business task."
-- We aren't cleaning them since they have no effect on KPIs

 /* {{{
-- station ids containing different values-
SELECT 
    CASE
        WHEN start_station_id REGEXP '^[0-9]+$' THEN 'Numeric'
        WHEN start_station_id REGEXP '^[A-Z]{2}[0-9]+$' THEN 'Alphanumeric'
        WHEN start_station_id = 'Unknown' THEN 'Unknown'
        ELSE 'Other'
    END AS id_type,
    COUNT(*) AS count
FROM combined_data
GROUP BY id_type;

-- With a total of 4,714,116 rows (per your latest count), the distribution shows:
 Alphanumeric: 1,962,991 rows (e.g., "TA1300..") – Likely the standard format, representing valid station IDs.            Alphanumeric: ~41.6%
Numeric_3-5: 1,813,725 rows (e.g., "123" or "12345") – A significant portion, possibly legacy IDs or truncated data.     Numeric_3-5: ~38.5%
Other: 249,733 rows – Miscellaneous formats, potentially errors or non-standard entries.                                 Other: ~5.3%
Unknown: 687,667 rows – Already imputed from your earlier cleaning.                                                      Unknown: ~14.6%

-- we aren't cleaning them as they have no effect on KPIs }}} 
*/

-- 8) Check for non-standard member_casual values
SELECT DISTINCT member_casual
FROM combined_data
WHERE member_casual NOT IN ('casual', 'member');
    -- As later on analysis it was seen that member_casual also had some wrong concatenated values like 'casual"E12D4A16BF51C274'
    -- (# count(DISTINCT member_casual)='779959') so we're just gonna update them. You can also use substr and replace to separate the values


-- ## Cleaning: Standardize member_casual
-- Fix concatenated anomalies in `member_casual` (e.g., 'casual"E12D4A16BF51C274').
-- Update to standardize
UPDATE combined_data
SET member_casual =
    CASE
        WHEN member_casual LIKE 'casual%' THEN 'casual'
        WHEN member_casual LIKE 'member%' THEN 'member'
        ELSE member_casual
    END;
    -- 4612303 row(s) affected Rows matched: 4714118 Changed: 4612303 Warnings: 0

-- ## 9. Analysis: Calculate Ride Length and Remove Outliers
-- Add `ride_length` column and remove invalid/outlier rides.

--  Calculating ride_length in minutes (if it's less than 0 min we'll delete it)
ALTER TABLE combined_data
ADD COLUMN ride_length DECIMAL;

UPDATE combined_data
SET ride_length = TIMESTAMPDIFF(MINUTE, started_at, ended_at);

DELETE FROM combined_data
WHERE ride_length <= 0;
    -- 99526 rows deleted (<0 minutes)

DELETE FROM combined_data
WHERE (ride_length / 60) > 24;
    -- Delete rows with >24 hrs; these are outliers. 209 row(s) affected. Now the total count is '4714116'

-- Descriptive stats
SELECT MAX(ride_length), MIN(ride_length), AVG(ride_length) AS avg_ride_length
FROM combined_data;
    -- Max(ride_length), min(ride_length), avg(ride_length)
    -- '1439', '1', '16.4923'
    -- Count(ride_length) - '1257' for 'casual' members where ride length>12hours. So not much among 4.8m cleaned records. Let it as it is

-- By member_casual
SELECT MAX(ride_length), MIN(ride_length), AVG(ride_length) AS avg_ride_length
FROM combined_data
WHERE member_casual = 'casual';
    -- '1439', '1', '22.2649'

SELECT MAX(ride_length), MIN(ride_length), AVG(ride_length) AS avg_ride_length
FROM combined_data
WHERE member_casual = 'member';
    -- '1435', '1', '12.3241'

-- Percentage difference
WITH averages AS (
    SELECT
        member_casual,
        AVG(ride_length) AS avg_ride_length_minutes
    FROM combined_data
    GROUP BY member_casual
)
SELECT
    a1.member_casual AS rider_type,
    a1.avg_ride_length_minutes,
    a2.avg_ride_length_minutes AS member_avg,
    ROUND(((a1.avg_ride_length_minutes - a2.avg_ride_length_minutes) / a2.avg_ride_length_minutes * 100), 2) AS percent_difference
FROM averages a1
CROSS JOIN averages a2
WHERE a1.member_casual = 'casual' AND a2.member_casual = 'member';
    -- Rider_type, avg_ride_length_minutes, member_avg, percent_difference
    -- 'casual', '22.2649', '12.3241', '80.66%'

-- ## 10. Analysis: Overall Ride Counts
SELECT member_casual, COUNT(*) AS rides
FROM combined_data
GROUP BY member_casual;
    -- Member_casual, rides
    -- 'casual', '1976664'
    -- 'member', '2737451'

-- ## 11. Analysis: Ride Frequency per User
SELECT member_casual,
       COUNT(DISTINCT ride_id) AS unique_users,
       COUNT(*) / COUNT(DISTINCT ride_id) AS avg_rides_per_user
FROM combined_data
GROUP BY member_casual;
    -- Member_casual, unique_users, avg_rides_per_user
    -- 'casual', '1976664', '1.0000'
    -- 'member', '2737451', '1.0000'

-- ## 12. Analysis: Rides by Day of Week
SELECT DAYNAME(started_at) AS day_of_week,
       member_casual,
       COUNT(*) AS rides
FROM combined_data
GROUP BY day_of_week, member_casual
ORDER BY FIELD(day_of_week, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');
    -- Day_of_week, member_casual, rides
    -- 'Monday', 'member', '381371'
    -- 'Monday', 'casual', '236306'
    -- 'Tuesday', 'member', '421920'
    -- 'Tuesday', 'casual', '225499'
    -- 'Wednesday', 'member', '435016'
    -- 'Wednesday', 'casual', '235556'
    -- 'Thursday', 'member', '436871'
    -- 'Thursday', 'casual', '262741'
    -- 'Friday', 'casual', '282842'
    -- 'Friday', 'member', '383295'
    -- 'Saturday', 'casual', '400364'
    -- 'Saturday', 'member', '361062'
    -- 'Sunday', 'member', '317916'
    -- 'Sunday', 'casual', '333356'

-- ## 13. Analysis: Top 10 Start Stations by Member Type
-- Top 10 start stations for casual vs members. Shows where to target ads near tourist stations (casual) vs commuter stations (members).
SELECT member_casual, start_station_name, COUNT(*) AS ride_count
FROM combined_data
GROUP BY member_casual, start_station_name
ORDER BY ride_count DESC
LIMIT 10;
    -- Member_casual, start_station_name, ride_count
    -- 'member', 'Unknown', '398186' -- unknown here were blank values
    -- 'casual', 'Unknown', '289481'
    -- 'casual', 'Streeter Dr & Grand Ave', '50745'
    -- 'casual', 'DuSable Lake Shore Dr & Monroe St', '27931'
    -- 'casual', 'Michigan Ave & Oak St', '22204'
    -- 'casual', 'Millennium Park', '22113'
    -- 'casual', 'DuSable Lake Shore Dr & North Blvd', '21043'
    -- 'member', 'Kingsbury St & Kinzie St', '20689'
    -- 'member', 'Clark St & Elm St', '18235'
    -- 'member', 'Wells St & Concord Ln', '17838'

-- For end_station_name (replace start_station_name with end_station_name)
-- Member_casual, end_station_name, ride_count
-- 'member', 'Unknown', '389766' -- you can not choose unknown name just avoid it
-- 'casual', 'Unknown', '334423'
-- 'casual', 'Streeter Dr & Grand Ave', '52543'
-- 'casual', 'DuSable Lake Shore Dr & Monroe St', '26016'
-- 'casual', 'DuSable Lake Shore Dr & North Blvd', '23649'
-- 'casual', 'Michigan Ave & Oak St', '23343'
-- 'casual', 'Millennium Park', '23324'
-- 'member', 'Kingsbury St & Kinzie St', '20371'
-- 'member', 'Clark St & Elm St', '18682'
-- 'member', 'Wells St & Concord Ln', '18384'

-- ## 14. Analysis: Rideable Type Preference
SELECT member_casual, rideable_type, COUNT(*) AS rides
FROM combined_data
GROUP BY member_casual, rideable_type
ORDER BY member_casual, rides DESC;


-- ##  Export Combined Dataset to CSV
-- Export the combined data to CSV for further use (e.g., in PowerBI). Note: No headers; saved to MySQL's default directory.

SELECT * FROM combined_data
INTO OUTFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads.csv'                    -- The directory is in MySQL program. Any file for export will get saved here then u can cut paste to desired location. Also it doesn't contain headers.
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';                                                                  -- After cleaning final row count 4714116
    


-- ## Summary
-- This script results in a cleaned dataset (`combined_data` with final count of 4,714,116 rows) ready for visualization in PowerBI or further analysis. 
-- Core insights: Casual rides 80.66% longer (22.26 min vs. 12.32 min), higher weekend usage, and tourist station preferences. 
