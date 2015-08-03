-- /Begin HACK create first table differently
-- README! why is the first table not done with CTAS (create table as), like the other tables?
--
-- hack for https://issues.apache.org/jira/browse/HIVE-2419 where CTAS (create table as) is not working for a fresh install where the "warehouse" folder for hive does not exist. 
-- The normal create table creates the warehouse folder if its missing.
-- But CTAS does not! create the warehouse folder, thus the "move" operation for data would fail with: 
-- "Failed with exception Unable to rename: hdfs://namenode:port/tmp/hive-root/../-ext-000001 hdfs://namenode:port/user/hive/warehouse/<database>/<table>"

DROP TABLE IF EXISTS createDatabaseDummyTable;
CREATE TABLE IF NOT EXISTS  createDatabaseDummyTable(  sk   bigint);
DROP TABLE IF EXISTS createDatabaseDummyTable;


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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/customer'
;


!echo Load text data into PARQUET table: customer;
CREATE TABLE IF NOT EXISTS customer
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/customer_address'
;

!echo Load text data into PARQUET table: customer_address;
CREATE TABLE IF NOT EXISTS customer_address
STORED AS PARQUET
AS
SELECT * FROM customer_address_temporary
;

!echo Drop temporary table: customer_address_temporary;
DROP TABLE IF EXISTS customer_address_temporary;


!echo Create temporary table: customer_demographics_temporary;
DROP TABLE IF EXISTS customer_demographics_temporary;
CREATE EXTERNAL TABLE customer_demographics_temporary
  ( cd_demo_sk                bigint                ----not null
  , cd_gender                 string
  , cd_marital_status         string
  , cd_education_status       string
  , cd_purchase_estimate      int
  , cd_credit_rating          string
  , cd_dep_count              int
  , cd_dep_employed_count     int
  , cd_dep_college_count      int

  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/customer_demographics'
;

!echo Load text data into PARQUET table: customer_demographics;
CREATE TABLE IF NOT EXISTS customer_demographics
STORED AS PARQUET
AS
SELECT * FROM customer_demographics_temporary
;

!echo Drop temporary table: customer_demographics_temporary;
DROP TABLE IF EXISTS customer_demographics_temporary;


!echo Create temporary table: date_dim_temporary;
DROP TABLE IF EXISTS date_dim_temporary;
CREATE EXTERNAL TABLE date_dim_temporary
  ( d_date_sk                 bigint              --not null
  , d_date_id                 string              --not null
  , d_date                    string
  , d_month_seq               int
  , d_week_seq                int
  , d_quarter_seq             int
  , d_year                    int
  , d_dow                     int
  , d_moy                     int
  , d_dom                     int
  , d_qoy                     int
  , d_fy_year                 int
  , d_fy_quarter_seq          int
  , d_fy_week_seq             int
  , d_day_name                string
  , d_quarter_name            string
  , d_holiday                 string
  , d_weekend                 string
  , d_following_holiday       string
  , d_first_dom               int
  , d_last_dom                int
  , d_same_day_ly             int
  , d_same_day_lq             int
  , d_current_day             string
  , d_current_week            string
  , d_current_month           string
  , d_current_quarter         string
  , d_current_year            string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/date_dim'
;

!echo Load text data into PARQUET table: date_dim;
CREATE TABLE IF NOT EXISTS date_dim
STORED AS PARQUET
AS
SELECT * FROM date_dim_temporary
;

!echo Drop temporary table: date_dim_temporary;
DROP TABLE IF EXISTS date_dim_temporary;


!echo Create temporary table: household_demographics_temporary;
DROP TABLE IF EXISTS household_demographics_temporary;
CREATE EXTERNAL TABLE household_demographics_temporary
  ( hd_demo_sk                bigint                --not null
  , hd_income_band_sk         bigint
  , hd_buy_potential          string
  , hd_dep_count              int
  , hd_vehicle_count          int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/household_demographics'
;

!echo Load text data into PARQUET table: household_demographics;
CREATE TABLE IF NOT EXISTS household_demographics
STORED AS PARQUET
AS
SELECT * FROM household_demographics_temporary
;

!echo Drop temporary table: household_demographics_temporary;
DROP TABLE IF EXISTS household_demographics_temporary;


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

!echo Drop temporary table: income_band_temporary;
DROP TABLE IF EXISTS income_band_temporary;


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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/item'
;

!echo Load text data into PARQUET table: item;
CREATE TABLE IF NOT EXISTS item
STORED AS PARQUET
AS
SELECT * FROM item_temporary
;

!echo Drop temporary table: item_temporary;
DROP TABLE IF EXISTS item_temporary;


!echo Create temporary table: promotion_temporary;
DROP TABLE IF EXISTS promotion_temporary;
CREATE EXTERNAL TABLE promotion_temporary
  ( p_promo_sk                bigint              --not null
  , p_promo_id                string              --not null
  , p_start_date_sk           bigint
  , p_end_date_sk             bigint
  , p_item_sk                 bigint
  , p_cost                    double
  , p_response_target         int
  , p_promo_name              string
  , p_channel_dmail           string
  , p_channel_email           string
  , p_channel_catalog         string
  , p_channel_tv              string
  , p_channel_radio           string
  , p_channel_press           string
  , p_channel_event           string
  , p_channel_demo            string
  , p_channel_details         string
  , p_purpose                 string
  , p_discount_active         string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/promotion'
;

!echo Load text data into PARQUET table: promotion;
CREATE TABLE IF NOT EXISTS promotion
STORED AS PARQUET
AS
SELECT * FROM promotion_temporary
;

!echo Drop temporary table: promotion_temporary;
DROP TABLE IF EXISTS promotion_temporary;


!echo Create temporary table: reason_temporary;
DROP TABLE IF EXISTS reason_temporary;
CREATE EXTERNAL TABLE reason_temporary
  ( r_reason_sk               bigint              --not null
  , r_reason_id               string              --not null
  , r_reason_desc             string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/reason'
;

!echo Load text data into PARQUET table: reason;
CREATE TABLE IF NOT EXISTS reason
STORED AS PARQUET
AS
SELECT * FROM reason_temporary
;

!echo Drop temporary table: reason_temporary;
DROP TABLE IF EXISTS reason_temporary;


!echo Create temporary table: ship_mode_temporary;
DROP TABLE IF EXISTS ship_mode_temporary;
CREATE EXTERNAL TABLE ship_mode_temporary
  ( sm_ship_mode_sk           bigint              --not null
  , sm_ship_mode_id           string              --not null
  , sm_type                   string
  , sm_code                   string
  , sm_carrier                string
  , sm_contract               string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/ship_mode'
;

!echo Load text data into PARQUET table: ship_mode;
CREATE TABLE IF NOT EXISTS ship_mode
STORED AS PARQUET
AS
SELECT * FROM ship_mode_temporary
;

!echo Drop temporary table: ship_mode_temporary;
DROP TABLE IF EXISTS ship_mode_temporary;


!echo Create temporary table: store_temporary;
DROP TABLE IF EXISTS store_temporary;
CREATE EXTERNAL TABLE store_temporary
  ( s_store_sk                bigint              --not null
  , s_store_id                string              --not null
  , s_rec_start_date          string
  , s_rec_end_date            string
  , s_closed_date_sk          bigint
  , s_store_name              string
  , s_number_employees        int
  , s_floor_space             int
  , s_hours                   string
  , s_manager                 string
  , s_market_id               int
  , s_geography_class         string
  , s_market_desc             string
  , s_market_manager          string
  , s_division_id             int
  , s_division_name           string
  , s_company_id              int
  , s_company_name            string
  , s_street_number           string
  , s_street_name             string
  , s_street_type             string
  , s_suite_number            string
  , s_city                    string
  , s_county                  string
  , s_state                   string
  , s_zip                     string
  , s_country                 string
  , s_gmt_offset              double
  , s_tax_precentage          double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/store'
;

!echo Load text data into PARQUET table: store;
CREATE TABLE IF NOT EXISTS store
STORED AS PARQUET
AS
SELECT * FROM store_temporary
;

!echo Drop temporary table: store_temporary;
DROP TABLE IF EXISTS store_temporary;


!echo Create temporary table: time_dim_temporary;
DROP TABLE IF EXISTS time_dim_temporary;
CREATE EXTERNAL TABLE time_dim_temporary
  ( t_time_sk                 bigint              --not null
  , t_time_id                 string              --not null
  , t_time                    int
  , t_hour                    int
  , t_minute                  int
  , t_second                  int
  , t_am_pm                   string
  , t_shift                   string
  , t_sub_shift               string
  , t_meal_time               string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/time_dim'
;

!echo Load text data into PARQUET table: time_dim;
CREATE TABLE IF NOT EXISTS time_dim
STORED AS PARQUET
AS
SELECT * FROM time_dim_temporary
;

!echo Drop temporary table: time_dim_temporary;
DROP TABLE IF EXISTS time_dim_temporary;


!echo Create temporary table: warehouse_temporary;
DROP TABLE IF EXISTS warehouse_temporary;
CREATE EXTERNAL TABLE warehouse_temporary
  ( w_warehouse_sk            bigint              --not null
  , w_warehouse_id            string              --not null
  , w_warehouse_name          string
  , w_warehouse_sq_ft         int
  , w_street_number           string
  , w_street_name             string
  , w_street_type             string
  , w_suite_number            string
  , w_city                    string
  , w_county                  string
  , w_state                   string
  , w_zip                     string
  , w_country                 string
  , w_gmt_offset              double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/warehouse'
;

!echo Load text data into PARQUET table: warehouse;
CREATE TABLE IF NOT EXISTS warehouse
STORED AS PARQUET
AS
SELECT * FROM warehouse_temporary
;

!echo Drop temporary table: warehouse_temporary;
DROP TABLE IF EXISTS warehouse_temporary;


!echo Create temporary table: web_site_temporary;
DROP TABLE IF EXISTS web_site_temporary;
CREATE EXTERNAL TABLE web_site_temporary
  ( web_site_sk               bigint              --not null
  , web_site_id               string              --not null
  , web_rec_start_date        string
  , web_rec_end_date          string
  , web_name                  string
  , web_open_date_sk          bigint
  , web_close_date_sk         bigint
  , web_class                 string
  , web_manager               string
  , web_mkt_id                int
  , web_mkt_class             string
  , web_mkt_desc              string
  , web_market_manager        string
  , web_company_id            int
  , web_company_name          string
  , web_street_number         string
  , web_street_name           string
  , web_street_type           string
  , web_suite_number          string
  , web_city                  string
  , web_county                string
  , web_state                 string
  , web_zip                   string
  , web_country               string
  , web_gmt_offset            double
  , web_tax_percentage        double
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/web_site'
;

!echo Load text data into PARQUET table: web_site;
CREATE TABLE IF NOT EXISTS web_site
STORED AS PARQUET
AS
SELECT * FROM web_site_temporary
;

!echo Drop temporary table: web_site_temporary;
DROP TABLE IF EXISTS web_site_temporary;


!echo Create temporary table: web_page_temporary;
DROP TABLE IF EXISTS web_page_temporary;
CREATE EXTERNAL TABLE web_page_temporary
  ( wp_web_page_sk            bigint              --not null
  , wp_web_page_id            string              --not null
  , wp_rec_start_date         string
  , wp_rec_end_date           string
  , wp_creation_date_sk       bigint
  , wp_access_date_sk         bigint
  , wp_autogen_flag           string
  , wp_customer_sk            bigint
  , wp_url                    string
  , wp_type                   string
  , wp_char_count             int
  , wp_link_count             int
  , wp_image_count            int
  , wp_max_ad_count           int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/web_page'
;

!echo Load text data into PARQUET table: web_page;
CREATE TABLE IF NOT EXISTS web_page
STORED AS PARQUET
AS
SELECT * FROM web_page_temporary
;

!echo Drop temporary table: web_page_temporary;
DROP TABLE IF EXISTS web_page_temporary;


!echo Create temporary table: inventory_temporary;
DROP TABLE IF EXISTS inventory_temporary;
CREATE EXTERNAL TABLE inventory_temporary
  ( inv_date_sk               bigint                --not null
  , inv_item_sk               bigint                --not null
  , inv_warehouse_sk          bigint                --not null
  , inv_quantity_on_hand      int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/inventory'
;

!echo Load text data into PARQUET table: inventory;
CREATE TABLE IF NOT EXISTS inventory
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/store_sales'
;

!echo Load text data into PARQUET table: store_sales;
CREATE TABLE IF NOT EXISTS store_sales
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/store_returns'
;

!echo Load text data into PARQUET table: store_returns;
CREATE TABLE IF NOT EXISTS store_returns
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/web_sales'
;

!echo Load text data into PARQUET table: web_sales;
CREATE TABLE IF NOT EXISTS web_sales
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/web_returns'
;

!echo Load text data into PARQUET table: web_returns;
CREATE TABLE IF NOT EXISTS web_returns
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/item_marketprices'
;

!echo Load text data into PARQUET table: item_marketprices;
CREATE TABLE IF NOT EXISTS item_marketprices
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/web_clickstreams'
;

!echo Load text data into PARQUET table: web_clickstreams;
CREATE TABLE IF NOT EXISTS web_clickstreams
STORED AS PARQUET
AS
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
  STORED AS TEXTFILE LOCATION '/user/root/benchmarks/bigbench/data/product_reviews'
;

!echo Load text data into PARQUET table: product_reviews;
CREATE TABLE IF NOT EXISTS product_reviews
STORED AS PARQUET
AS
SELECT * FROM product_reviews_temporary
;

!echo Drop temporary table: product_reviews_temporary;
DROP TABLE IF EXISTS product_reviews_temporary;
