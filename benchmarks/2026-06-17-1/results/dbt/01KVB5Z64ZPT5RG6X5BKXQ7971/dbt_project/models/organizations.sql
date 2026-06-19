{{ config(materialized='table') }}

SELECT
    id::text AS sf_account_id,
    TRIM(name)::text AS name,
    CONCAT_WS(', ',
        NULLIF(TRIM(billing_street), ''),
        NULLIF(TRIM(billing_city), ''),
        NULLIF(TRIM(billing_state), ''),
        NULLIF(TRIM(billing_postal_code), ''),
        NULLIF(TRIM(billing_country), '')
    )::text AS address,
    CASE 
        WHEN owner_id ~ '^[0-9]+$' THEN owner_id::integer 
        ELSE NULL 
    END AS owner_id,
    created_date::timestamp AS add_time
FROM {{ source('src_01kvb5z64zpt5rg6x5bkxq7971_raw', 'account') }}
WHERE id IS NOT NULL