{{ config(materialized='table') }}

WITH raw_accounts AS (
    SELECT * FROM {{ source('salesforce', 'account') }}
)

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    TRIM(
        CONCAT_WS(', ', 
            NULLIF(billing_street, ''), 
            NULLIF(billing_city, ''), 
            NULLIF(billing_state, ''), 
            NULLIF(billing_postal_code, ''), 
            NULLIF(billing_country, '')
        )
    ) AS address,
    CASE 
        WHEN owner_id ~ '^\d+$' THEN owner_id::INTEGER 
        ELSE NULL 
    END AS owner_id,
    created_date AS add_time
FROM raw_accounts
