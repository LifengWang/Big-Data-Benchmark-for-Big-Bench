INSERT INTO testtwo
SELECT *
  FROM (
    SELECT
      ws.ws_order_number AS ordernumber,
      i.i_category_id AS category_id,
      i.i_category AS category
    FROM web_sales ws
    JOIN item i ON (ws.ws_item_sk = i.i_item_sk
    AND i.i_category_id IS NOT NULL)
    CLUSTER BY ordernumber
  ) mo;
