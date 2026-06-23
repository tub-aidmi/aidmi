{{ config(materialized='table') }}

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(', ', billing_street, billing_city, billing_state, billing_postal_code, billing_country) AS address,
    CASE 
        WHEN TRIM(owner_id) ~ '^\d+$' THEN TRIM(owner_id)::integer 
        ELSE NULL 
    END AS owner_id,
    created_date::timestamp AS add_time
FROM {{ source('src_01kvbnkyxk6zmmxvjzrpstagxe_raw', 'account') }}
WHERE is_deleted IS NOT TRUE