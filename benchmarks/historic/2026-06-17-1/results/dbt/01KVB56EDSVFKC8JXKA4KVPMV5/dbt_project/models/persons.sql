{{ config(materialized='table') }}

SELECT
    con.id AS sf_contact_ID,
    NULLIF(TRIM(CONCAT_WS(' ', con.first_name, con.last_name)), '') AS name,
    LOWER(TRIM(con.email)) AS email,
    COALESCE(con.phone, con.mobile_phone) AS phone,
    TRIM(acc.name) AS org_name,
    CASE 
        WHEN con.owner_id ~ '^[0-9]+$' THEN con.owner_id::integer 
        ELSE NULL 
    END AS owner_id,
    con.created_date AS add_time
FROM {{ source('src_01kvb56edsvfkc8jxka4kvpmv5_raw', 'contact') }} con
LEFT JOIN {{ source('src_01kvb56edsvfkc8jxka4kvpmv5_raw', 'account') }} acc
    ON con.account_id = acc.id