{{ config(materialized='table') }}

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(
        ', ',
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country
    ) AS address,
    CASE 
        WHEN owner_id ~ '^[0-9]+$' THEN CAST(owner_id AS integer) 
        ELSE NULL 
    END AS owner_id,
    created_date AS add_time
FROM {{ source('src_01kvb56edsvfkc8jxka4kvpmv5_raw', 'account') }}