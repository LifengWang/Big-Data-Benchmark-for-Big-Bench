--"INTEL CONFIDENTIAL"
--Copyright 2015  Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

-- TASK:
--Find the last 5 products that are mostly viewed before a given product
--was purchased online. Only products in certain item categories and viewed within 10
--days before the purchase date are considered. 


--IMPLEMENTATION NOTICE: 
-- The task exceeds "click session" boundaries: all clicks of a user wihtin the 10 day before purchase time frame have to be considered.
-- Theoretically you could view this task as a "market basket analysis" with a very large basket (all clicks of a user for every purchase), which would be inefficient.
-- This is a classic MR filtering job which cannot be easily expressed and execxuted efficiently in hive/sql. 
-- This does not mean you cant express the job purely in HQL. By cleverly employing windowing functions with "preceeding" rows and "lag" it can be achieved.
-- However this implementation  uses a custom reducer streaming job script, which enforces the "last 10 days" and "last 5 views" constraints in a sequential fashion, not requireing excessive caching or joining.
-- The reduce script requires the input to be pre-partitioned by user_sk and pre-sorted on timestamp by hive.


-- Resources
ADD FILE ${hiveconf:QUERY_DIR}/q03_filterLast_N_viewedItmes_within_y_days.py;

--Result -------------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  lastviewed_item BIGINT,
  purchased_item  BIGINT,
  cnt             BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT purchased_item , lastviewed_item,  count(*) as cnt
FROM
	(
	SELECT * 
	FROM item i ,
		(--sessionize and filter "last 5 viewed products after purchase" with reduce script
		  FROM 
			  (
				SELECT
				  wcs_user_sk  ,
				  wcs_click_date_sk as tstamp ,
				  wcs_item_sk  ,     
				  wcs_sales_sk     
				FROM web_clickstreams w
				WHERE wcs_user_sk IS NOT NULL -- only select clickstreams resulting in a purchase user_sk = null -> only non buying visitor
				AND wcs_item_sk IS NOT NULL
				DISTRIBUTE BY wcs_user_sk --build clickstream per user
				SORT BY tstamp DESC --order by tstamp => required by python script
			  ) q03_map_output
		  REDUCE
			  q03_map_output.wcs_user_sk,
			  q03_map_output.tstamp,
			  q03_map_output.wcs_item_sk,
			  q03_map_output.wcs_sales_sk
		  --Reducer script logic: iterate through clicks of a user in descending order (most recent click first).
		  --if a purchase is found (wcs_sales_sk!=null) display the next 5 clicks if they are within the provided date range (max 10 days before)
		  --Reducer script selects only:
		  -- * products viewed within 'q03_days_before_purchase' days before the purchase date
		  -- * only the last 5 products that where purchased before a sale
		  USING 'python q03_filterLast_N_viewedItmes_within_y_days.py ${hiveconf:q03_days_before_purchase} ${hiveconf:q03_views_before_purchase}'
		  AS ( purchased_item BIGINT, lastviewed_item BIGINT)
		) lastViewSessions
	WHERE i.i_item_sk = lastViewSessions.lastviewed_item
	AND i.i_category_id IN (${hiveconf:q03_purchased_item_category_IN})  --Only products in certain categories			
	AND purchased_item IN ( ${hiveconf:q03_purchased_item_IN} )
	CLUSTER BY lastviewed_item,purchased_item -- pre cluster to speed up following group by and count()
	) distributed
GROUP BY lastviewed_item,purchased_item
ORDER BY cnt DESC, purchased_item, lastviewed_item
LIMIT 100
--DISTRIBUTE BY lastviewed_item SORT BY cnt DESC, purchased_item, lastviewed_item --cluster parallel sorting
;
