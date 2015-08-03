--"INTEL CONFIDENTIAL"
--Copyright 2015  Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.


--Perform category affinity analysis for products viewed together.

-- Resources
ADD FILE /root/bb_on_mapreduce/engines/hive/queries/q30/reducer_q30.py;
--ADD FILE ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar; 

--Result  --------------------------------------------------------------------
--keep result human readable
--set hive.exec.compress.output=false;
--set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS q30_hive_POWER_TEST_IN_PROGRESS_0_result;
CREATE TABLE q30_hive_POWER_TEST_IN_PROGRESS_0_result (
  category_id        STRING,
  affine_category_id STRING,
  category           STRING,
  affine_category    STRING,
  frequency          BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/queryResults/q30_hive_POWER_TEST_IN_PROGRESS_0_result';

-- Begin: the real query part
INSERT INTO TABLE q30_hive_POWER_TEST_IN_PROGRESS_0_result
SELECT
  ro.category_id AS category_id,
  ro.affine_category_id AS affine_category_id,
  ro.category AS category,
  ro.affine_category AS affine_category,
  count(*) as frequency
FROM (
  FROM (
    SELECT
      concat(wcs.wcs_user_sk, ':', wcs.wcs_click_date_sk) AS combined_key,
      i.i_category_id AS category_id,
      i.i_category AS category
    FROM web_clickstreams wcs
    JOIN item i ON (wcs.wcs_item_sk = i.i_item_sk AND i.i_category_id IS NOT NULL)
    AND wcs.wcs_user_sk IS NOT NULL
    AND wcs.wcs_item_sk IS NOT NULL
    CLUSTER BY combined_key
  ) mo
  REDUCE
    mo.combined_key,
    mo.category_id,
    mo.category
  USING 'python reducer_q30.py'
  AS (
    category_id,
    category,
    affine_category_id,
    affine_category )
) ro
GROUP BY ro.category_id , ro.affine_category_id, ro.category ,ro.affine_category
CLUSTER BY frequency
;
