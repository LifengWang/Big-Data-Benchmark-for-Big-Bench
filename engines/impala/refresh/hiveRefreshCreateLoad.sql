!echo Create temporary table: customer_temporary;
DROP TABLE IF EXISTS customer_temporary;
CREATE EXTERNAL TABLE customer_temporary
  ( c_customer_sk             bigint              --not null
  , c_customer_id             string              --not null
  , c_current_cdemo_sk        bigint
  , c_current_hdemo_sk        bigint
  , c_current_addr_sk         bigint
  , c_first_shipto_date_sk    bigint
  , c_first_sales_date_sk     bigint
  , c_salutation              string
  , c_first_name              string
  , c_last_name               string
  , c_preferred_cust_flag     string
  , c_birth_day               int
  , c_birth_month             int
  , c_birth_year              int
  , c_birth_country           string
  , c_login                   string
  , c_email_address           string
  , c_last_review_date        string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/customer'
;

!echo Load text data into table: customer;
INSERT INTO TABLE customer
SELECT * FROM customer_temporary
;

!echo Drop temporary table: customer_temporary;
DROP TABLE IF EXISTS customer_temporary;


!echo Create temporary table: customer_address_temporary;
DROP TABLE IF EXISTS customer_address_temporary;
CREATE EXTERNAL TABLE customer_address_temporary
  ( ca_address_sk             bigint              --not null
  , ca_address_id             string              --not null
  , ca_street_number          string
  , ca_street_name            string
  , ca_street_type            string
  , ca_suite_number           string
  , ca_city                   string
  , ca_county                 string
  , ca_state                  string
  , ca_zip                    string
  , ca_country                string
  , ca_gmt_offset             double
  , ca_location_type          string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/customer_address'
;

!echo Load text data into table: customer_address;
INSERT INTO TABLE customer_address
SELECT * FROM customer_address_temporary
;

!echo Drop temporary table: customer_address_temporary;
DROP TABLE IF EXISTS customer_address_temporary;


!echo Create temporary table: item_temporary;
DROP TABLE IF EXISTS item_temporary;
CREATE EXTERNAL TABLE item_temporary
  ( i_item_sk                 bigint              --not null
  , i_item_id                 string              --not null
  , i_rec_start_date          string
  , i_rec_end_date            string
  , i_item_desc               string
  , i_current_price           double
  , i_wholesale_cost          double
  , i_brand_id                int
  , i_brand                   string
  , i_class_id                int
  , i_class                   string
  , i_category_id             int
  , i_category                string
  , i_manufact_id             int
  , i_manufact                string
  , i_size                    string
  , i_formulation             string
  , i_color                   string
  , i_units                   string
  , i_container               string
  , i_manager_id              int
  , i_product_name            string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/item'
;

!echo Load text data into table: item;
INSERT INTO TABLE item
SELECT * FROM item_temporary
;

!echo Drop temporary table: item_temporary;
DROP TABLE IF EXISTS item_temporary;


!echo Create temporary table: inventory_temporary;
DROP TABLE IF EXISTS inventory_temporary;
CREATE EXTERNAL TABLE inventory_temporary
  ( inv_date_sk               bigint                --not null
  , inv_item_sk               bigint                --not null
  , inv_warehouse_sk          bigint                --not null
  , inv_quantity_on_hand      int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/inventory'
;

!echo Load text data into table: inventory;
INSERT INTO TABLE inventory
SELECT * FROM inventory_temporary
;

!echo Drop temporary table: inventory_temporary;
DROP TABLE IF EXISTS inventory_temporary;


!echo Create temporary table: store_sales_temporary;
DROP TABLE IF EXISTS store_sales_temporary;
CREATE EXTERNAL TABLE store_sales_temporary
  ( ss_sold_date_sk           bigint
  , ss_sold_time_sk           bigint
  , ss_item_sk                bigint                --not null
  , ss_customer_sk            bigint
  , ss_cdemo_sk               bigint
  , ss_hdemo_sk               bigint
  , ss_addr_sk                bigint
  , ss_store_sk               bigint
  , ss_promo_sk               bigint
  , ss_ticket_number          bigint                --not null
  , ss_quantity               int
  , ss_wholesale_cost         double
  , ss_list_price             double
  , ss_sales_price            double
  , ss_ext_discount_amt       double
  , ss_ext_sales_price        double
  , ss_ext_wholesale_cost     double
  , ss_ext_list_price         double
  , ss_ext_tax                double
  , ss_coupon_amt             double
  , ss_net_paid               double
  , ss_net_paid_inc_tax       double
  , ss_net_profit             double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/store_sales'
;

!echo Load text data into table: store_sales;
INSERT INTO TABLE store_sales
SELECT * FROM store_sales_temporary
;

!echo Drop temporary table: store_sales_temporary;
DROP TABLE IF EXISTS store_sales_temporary;


!echo Create temporary table: store_returns_temporary;
DROP TABLE IF EXISTS store_returns_temporary;
CREATE EXTERNAL TABLE store_returns_temporary
  ( sr_returned_date_sk       bigint
  , sr_return_time_sk         bigint
  , sr_item_sk                bigint                --not null
  , sr_customer_sk            bigint
  , sr_cdemo_sk               bigint
  , sr_hdemo_sk               bigint
  , sr_addr_sk                bigint
  , sr_store_sk               bigint
  , sr_reason_sk              bigint
  , sr_ticket_number          bigint                --not null
  , sr_return_quantity        int
  , sr_return_amt             double
  , sr_return_tax             double
  , sr_return_amt_inc_tax     double
  , sr_fee                    double
  , sr_return_ship_cost       double
  , sr_refunded_cash          double
  , sr_reversed_charge        double
  , sr_store_credit           double
  , sr_net_loss               double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/store_returns'
;

!echo Load text data into table: store_returns;
INSERT INTO TABLE store_returns
SELECT * FROM store_returns_temporary
;

!echo Drop temporary table: store_returns_temporary;
DROP TABLE IF EXISTS store_returns_temporary;


!echo Create temporary table: web_sales_temporary;
DROP TABLE IF EXISTS web_sales_temporary;
CREATE EXTERNAL TABLE web_sales_temporary
  ( ws_sold_date_sk           bigint
  , ws_sold_time_sk           bigint
  , ws_ship_date_sk           bigint
  , ws_item_sk                bigint                --not null
  , ws_bill_customer_sk       bigint
  , ws_bill_cdemo_sk          bigint
  , ws_bill_hdemo_sk          bigint
  , ws_bill_addr_sk           bigint
  , ws_ship_customer_sk       bigint
  , ws_ship_cdemo_sk          bigint
  , ws_ship_hdemo_sk          bigint
  , ws_ship_addr_sk           bigint
  , ws_web_page_sk            bigint
  , ws_web_site_sk            bigint
  , ws_ship_mode_sk           bigint
  , ws_warehouse_sk           bigint
  , ws_promo_sk               bigint
  , ws_order_number           bigint                --not null
  , ws_quantity               int
  , ws_wholesale_cost         double
  , ws_list_price             double
  , ws_sales_price            double
  , ws_ext_discount_amt       double
  , ws_ext_sales_price        double
  , ws_ext_wholesale_cost     double
  , ws_ext_list_price         double
  , ws_ext_tax                double
  , ws_coupon_amt             double
  , ws_ext_ship_cost          double
  , ws_net_paid               double
  , ws_net_paid_inc_tax       double
  , ws_net_paid_inc_ship      double
  , ws_net_paid_inc_ship_tax  double
  , ws_net_profit             double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/web_sales'
;

!echo Load text data into table: web_sales;
INSERT INTO TABLE web_sales
SELECT * FROM web_sales_temporary
;

!echo Drop temporary table: web_sales_temporary;
DROP TABLE IF EXISTS web_sales_temporary;


!echo Create temporary table: web_returns_temporary;
DROP TABLE IF EXISTS web_returns_temporary;
CREATE EXTERNAL TABLE web_returns_temporary
  ( wr_returned_date_sk       bigint 
  , wr_returned_time_sk       bigint
  , wr_item_sk                bigint                --not null
  , wr_refunded_customer_sk   bigint
  , wr_refunded_cdemo_sk      bigint
  , wr_refunded_hdemo_sk      bigint
  , wr_refunded_addr_sk       bigint
  , wr_returning_customer_sk  bigint
  , wr_returning_cdemo_sk     bigint
  , wr_returning_hdemo_sk     bigint
  , wr_returning_addr_sk      bigint
  , wr_web_page_sk            bigint
  , wr_reason_sk              bigint
  , wr_order_number           bigint                --not null
  , wr_return_quantity        int
  , wr_return_amt             double
  , wr_return_tax             double
  , wr_return_amt_inc_tax     double
  , wr_fee                    double
  , wr_return_ship_cost       double
  , wr_refunded_cash          double
  , wr_reversed_charge        double
  , wr_account_credit         double
  , wr_net_loss               double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/web_returns'
;

!echo Load text data into table: web_returns;
INSERT INTO TABLE web_returns
SELECT * FROM web_returns_temporary
;

!echo Drop temporary table: web_returns_temporary;
DROP TABLE IF EXISTS web_returns_temporary;


!echo Create temporary table: item_marketprices_temporary;
DROP TABLE IF EXISTS item_marketprices_temporary;
CREATE EXTERNAL TABLE item_marketprices_temporary
  ( imp_sk                  bigint                --not null
  , imp_item_sk             bigint                --not null
  , imp_competitor          string
  , imp_competitor_price    double
  , imp_start_date          bigint
  , imp_end_date            bigint

  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/item_marketprices'
;

!echo Load text data into table: item_marketprices;
INSERT INTO TABLE item_marketprices
SELECT * FROM item_marketprices_temporary
;

!echo Drop temporary table: item_marketprices_temporary;
DROP TABLE IF EXISTS item_marketprices_temporary;


!echo Create temporary table: web_clickstreams_temporary;
DROP TABLE IF EXISTS web_clickstreams_temporary;
CREATE EXTERNAL TABLE web_clickstreams_temporary
(   wcs_click_date_sk       bigint
  , wcs_click_time_sk       bigint
  , wcs_sales_sk            bigint
  , wcs_item_sk             bigint
  , wcs_web_page_sk         bigint
  , wcs_user_sk             bigint
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/web_clickstreams'
;

!echo Load text data into table: web_clickstreams;
INSERT INTO TABLE web_clickstreams
SELECT * FROM web_clickstreams_temporary
;

!echo Drop temporary table: web_clickstreams_temporary;
DROP TABLE IF EXISTS web_clickstreams_temporary;


!echo Create temporary table: product_reviews_temporary;
DROP TABLE IF EXISTS product_reviews_temporary;
CREATE EXTERNAL TABLE product_reviews_temporary
(   pr_review_sk            bigint              --not null
  , pr_review_date          string
  , pr_review_time          string 
  , pr_review_rating        int                 --not null
  , pr_item_sk              bigint              --not null
  , pr_user_sk              bigint
  , pr_order_sk             bigint
  , pr_review_content       string --not null
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data_refresh/product_reviews'
;

!echo Load text data into table: product_reviews;
INSERT INTO TABLE product_reviews
SELECT * FROM product_reviews_temporary
;

!echo Drop temporary table: product_reviews_temporary;
DROP TABLE IF EXISTS product_reviews_temporary;
