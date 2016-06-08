--"INTEL CONFIDENTIAL"
--Copyright 2015  Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.


--Extract competitor product names and model names (if any) from
--online product reviews for a given product.

-- Resources
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-maxent-3.0.3.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-tools-1.5.3.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION find_company AS 'io.bigdatabenchmark.v1.queries.q27.CompanyUDF';

-- !echo Extract competitor product names and model names (if any) from online product reviews for a given product: (item_sk: '10653');

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q27_hive_POWER_TEST_IN_PROGRESS_0_result;
CREATE TABLE q27_hive_POWER_TEST_IN_PROGRESS_0_result (
  pr_review_sk    BIGINT,
  pr_item_sk      BIGINT,
  company_name    STRING,
  review_sentence STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/queryResults/q27_hive_POWER_TEST_IN_PROGRESS_0_result';

-- the real query part
INSERT INTO TABLE q27_hive_POWER_TEST_IN_PROGRESS_0_result
SELECT find_company(pr_review_sk, pr_item_sk, pr_review_content) AS (pr_review_sk, pr_item_sk, company_name, review_sentence)
FROM (
  SELECT pr_review_sk, pr_item_sk, pr_review_content
  FROM product_reviews
  WHERE pr_item_sk = 10653
) subtable
;