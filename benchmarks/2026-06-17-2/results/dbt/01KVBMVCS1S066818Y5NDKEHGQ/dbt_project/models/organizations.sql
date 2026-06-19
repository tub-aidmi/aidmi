{{ config(materialized='table') }}

SELECT
    COALESCE(id::text, 'UNKNOWN_PLACEHOLDER') AS sf_account_id,
    COALESCE(trim(name), 'Unknown') AS name,
    trim(concat_ws(', ', billing_street, billing_city, billing_state, billing_postal_code, billing_country)) AS address,
    CASE 
        WHEN owner_id ~ '^[0-9]+$' THEN owner_id::integer 
        ELSE NULL 
    END AS owner_id,
    created_date AS add_time
FROM {{ source('src_01kvbmvcs1s066818y5ndkehgq_raw', 'account') }}