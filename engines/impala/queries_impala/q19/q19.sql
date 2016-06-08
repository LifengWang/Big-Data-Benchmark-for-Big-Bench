--"INTEL CONFIDENTIAL"
--Copyright 2015  Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.


--Retrieve the items with the highest number of returns where the num-
--ber of returns was approximately equivalent across all store and web channels
--(within a tolerance of +/- 10%), within the week ending a given date. Analyze
--the online reviews for these items to see if there are any major negative reviews.

-- Resources

-----Store returns in date range q19_tmp_date1-------------------------------------------------
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_sr_items;
CREATE VIEW q19_hive_POWER_TEST_IN_PROGRESS_0_temp_sr_items AS
SELECT
  i_item_sk item_id,
  SUM(sr_return_quantity) sr_item_qty
FROM
  store_returns sr
INNER JOIN item i ON sr.sr_item_sk = i.i_item_sk
INNER JOIN date_dim d ON sr.sr_returned_date_sk = d.d_date_sk
INNER JOIN (
  SELECT d1.d_date_sk
  FROM date_dim d1
  LEFT SEMI JOIN date_dim d2 ON (
    d1.d_week_seq = d2.d_week_seq
    AND d2.d_date IN ( '2001-01-02','2001-10-15','2001-11-10' )
  )
) d1 ON sr.sr_returned_date_sk = d1.d_date_sk
GROUP BY i_item_sk
HAVING SUM(sr_return_quantity) > 0
;


-----Web returns in date range q19_tmp_date2 ------------------------------------------------------
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_wr_items;
--make q19_hive_POWER_TEST_IN_PROGRESS_0_temp_wr_items
CREATE VIEW q19_hive_POWER_TEST_IN_PROGRESS_0_temp_wr_items AS
SELECT
  i_item_sk item_id,
  SUM(wr_return_quantity) wr_item_qty
FROM
  web_returns wr
INNER JOIN item i ON wr.wr_item_sk = i.i_item_sk
INNER JOIN date_dim d ON wr.wr_returned_date_sk = d.d_date_sk
INNER JOIN (
  SELECT d1.d_date_sk
  FROM date_dim d1
  LEFT SEMI JOIN date_dim d2 ON (
    d1.d_week_seq = d2.d_week_seq 
    AND d2.d_date IN ( '2004-03-10' ,'2004-08-04' ,'2004-11-14' )
  )
) d2 ON wr.wr_returned_date_sk = d2.d_date_sk
GROUP BY i_item_sk
HAVING SUM(wr_return_quantity) > 0
;


----return items -------------------------------------------------------------------
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_return_items;
--make q19_hive_POWER_TEST_IN_PROGRESS_0_temp_return_items
CREATE VIEW q19_hive_POWER_TEST_IN_PROGRESS_0_temp_return_items AS
SELECT
  st.item_id item,
  sr_item_qty,
  100.0 * sr_item_qty / (sr_item_qty+wr_item_qty) / 2.0 sr_dev,
  wr_item_qty,
  100.0 * wr_item_qty / (sr_item_qty+wr_item_qty) / 2.0 wr_dev,
  (sr_item_qty + wr_item_qty) / 2.0 average
FROM q19_hive_POWER_TEST_IN_PROGRESS_0_temp_sr_items st
INNER JOIN q19_hive_POWER_TEST_IN_PROGRESS_0_temp_wr_items wt ON st.item_id = wt.item_id
--CLUSTER BY average desc
ORDER BY average DESC
LIMIT 100
;

---Sentiment analysis and Result----------------------------------------------------------------
--- we can reuse the  sentiment analysis helper class from q10
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-maxent-3.0.3.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-tools-1.5.3.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF';


--Result  returned items with negative sentiment --------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_result;
CREATE TABLE q19_hive_POWER_TEST_IN_PROGRESS_0_result (
  pr_item_sk      BIGINT,
  review_sentence STRING,
  sentiment       STRING,
  sentiment_word  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/queryResults/q19_hive_POWER_TEST_IN_PROGRESS_0_result';

---- the real query --------------
INSERT INTO TABLE q19_hive_POWER_TEST_IN_PROGRESS_0_result
SELECT *
FROM
(
  SELECT extract_sentiment(pr.pr_item_sk, pr.pr_review_content) AS (
    pr_item_sk,
    review_sentence,
    sentiment,
    sentiment_word
  )
  FROM product_reviews pr
  LEFT SEMI JOIN q19_hive_POWER_TEST_IN_PROGRESS_0_temp_return_items ri ON pr.pr_item_sk = ri.item
) q19_tmp_sentiment
WHERE q19_tmp_sentiment.sentiment = 'NEG';


--- cleanup---------------------------------------------------------------------------
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_sr_items;
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_wr_items;
DROP VIEW IF EXISTS q19_hive_POWER_TEST_IN_PROGRESS_0_temp_return_items;