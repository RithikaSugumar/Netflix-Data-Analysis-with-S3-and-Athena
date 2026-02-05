CREATE EXTERNAL TABLE netflix_processed (
  show_id string,
  type string,
  title string,
  director string,
  cast_member string,
  country string,
  date_added string,
  release_year int,
  rating string,
  duration string,
  listed_in string,
  description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://<your-bucket>/Unsaved/2026/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);
