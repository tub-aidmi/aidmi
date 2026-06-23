{{ config(materialized='table') }}

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(', ',
        NULLIF(TRIM(billing_street), ''),
        NULLIF(TRIM(billing_city), ''),
        NULLIF(TRIM(billing_state), ''),
        NULLIF(TRIM(billing_postal_code), ''),
        NULLIF(TRIM(billing_country), '')
    ) AS address,
    CASE 
        WHEN owner_id ~ '^[0-9]+$' THEN owner_id::integer 
        ELSE NULL 
    END AS owner_id,
    created_date AS add_time
FROM {{ source('src_01kvb368f3v712ygm4x0r91803_raw', 'account') }}
WHERE is_deleted IS NOT TRUE