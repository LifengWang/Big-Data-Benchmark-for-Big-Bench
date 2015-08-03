DROP TABLE IF EXISTS q14_hive_POWER_TEST_IN_PROGRESS_0_result;
CREATE TABLE q14_hive_POWER_TEST_IN_PROGRESS_0_result (
  am_pm_ratio DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/queryResults/q14_hive_POWER_TEST_IN_PROGRESS_0_result';

SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
FROM (
  SELECT COUNT(*) amc
  FROM web_sales ws
  JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
  AND hd.hd_dep_count = 5
) at
JOIN (
  SELECT COUNT(*) pmc
  FROM web_sales ws
  JOIN household_demographics hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
  AND hd.hd_dep_count = 5
) pt
ORDER BY am_pm_ratio
;
