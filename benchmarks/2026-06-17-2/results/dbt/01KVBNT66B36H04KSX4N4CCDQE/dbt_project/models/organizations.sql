{{ config(materialized='table') }}

SELECT
    id::text AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(', ', TRIM(billing_street), TRIM(billing_city), TRIM(billing_state), TRIM(billing_postal_code), TRIM(billing_country)) AS address,
    CASE 
        WHEN owner_id IS NOT NULL AND trim(owner_id) ~ '^\d+$' THEN trim(owner_id)::INTEGER 
        ELSE NULL 
    END AS owner_id,
    created_date::timestamp AS add_time
FROM {{ source('src_01kvbnt66b36h04ksx4n4ccdqe_raw', 'account') }}
WHERE is_deleted = FALSE OR is_deleted IS NULL