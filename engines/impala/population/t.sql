!echo Create temporary table: income_band_temporary;
DROP TABLE IF EXISTS income_band_temporary;
CREATE EXTERNAL TABLE income_band_temporary
  ( ib_income_band_sk         bigint              --not null
  , ib_lower_bound            int
  , ib_upper_bound            int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/income_band'
;

!echo Load text data into PARQUET table: income_band;
CREATE TABLE IF NOT EXISTS income_band
STORED AS PARQUET
AS
SELECT * FROM income_band_temporary
;
