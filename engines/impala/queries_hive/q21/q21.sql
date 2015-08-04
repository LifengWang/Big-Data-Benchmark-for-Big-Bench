--"INTEL CONFIDENTIAL"
--Copyright 2015  Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

--based on tpc-ds q29
--Get all items that were sold in stores in a given month
--and year and which were returned in the next 6 months and re-purchased by
--the returning customer afterwards through the web sales channel in the following
--three years. For those these items, compute the total quantity sold through the
--store, the quantity returned and the quantity purchased through the web. Group
--this information by item and store.


--Result --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  item_id                STRING,
  item_desc              STRING,
  store_id               STRING,
  store_name             STRING,
  store_sales_quantity   BIGINT,
  store_returns_quantity BIGINT,
  web_sales_quantity     BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
select   
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,sum(ss_quantity)        as store_sales_quantity
    ,sum(sr_return_quantity) as store_returns_quantity
    ,sum(ws_quantity)        as web_sales_quantity
 from
    store_sales
   ,store_returns
   ,web_sales
   ,date_dim             d1
   ,date_dim             d2
   ,date_dim             d3
   ,store
   ,item
 where   d1.d_year          = ${hiveconf:q21_year} --sold in stores in a given month and year
 and    d1.d_moy            = ${hiveconf:q21_month}
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between ${hiveconf:q21_month} and  ${hiveconf:q21_month} + 6 --which were returned in the next six months 
 and d2.d_year              = ${hiveconf:q21_year}
 and sr_customer_sk         = ws_bill_customer_sk --re-purchased by the returning customer afterwards through the web sales channel
 and sr_item_sk             = ws_item_sk
 and ws_sold_date_sk        = d3.d_date_sk     
 and d3.d_year              between ${hiveconf:q21_year} and ${hiveconf:q21_year} + 2 -- in the following three years (re-purchased by the returning customer afterwards through the web sales channel) 
 group by
    i_item_id
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 order by
    i_item_id 
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 limit ${hiveconf:q21_limit};


