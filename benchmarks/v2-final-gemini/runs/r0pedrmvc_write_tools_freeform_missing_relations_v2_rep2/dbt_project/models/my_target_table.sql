{{ config(materialized='table') }}

SELECT
    CAST(id AS INTEGER) AS "Id",
    TRIM(customer_name) AS "CustomerName",
    CASE
        WHEN order_date_str ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(order_date_str AS DATE)
        WHEN order_date_str ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(order_date_str, 'DD.MM.YYYY')
        WHEN order_date_str ~ '^\d{8}$' THEN TO_DATE(order_date_str, 'YYYYMMDD')
        ELSE NULL
    END AS "OrderDate",
    CASE
        WHEN amount_str ~ '^\$[0-9,]+\.?\d*$' THEN CAST(REPLACE(REPLACE(amount_str, '$', ''), ',', '') AS NUMERIC) -- US format $1,234.56
        WHEN amount_str ~ '^[0-9\.]+,\d{2}( EUR)?$' THEN CAST(REPLACE(REPLACE(amount_str, '.', ''), ',', '.') AS NUMERIC) -- European format 1.234,56 EUR
        WHEN amount_str ~ '^[0-9]+(\.?\d+)?$' THEN CAST(amount_str AS NUMERIC) -- Plain number
        ELSE NULL
    END AS "OrderAmount",
    CASE
        WHEN LOWER(status_code) = 'act' THEN 'Active'
        WHEN LOWER(status_code) = 'inact' THEN 'Inactive'
        ELSE 'Unknown'
    END AS "Status",
    CAST(id AS TEXT) AS "Legacy_Id__c"
FROM
    {{ source('my_source', 'some_table') }}
