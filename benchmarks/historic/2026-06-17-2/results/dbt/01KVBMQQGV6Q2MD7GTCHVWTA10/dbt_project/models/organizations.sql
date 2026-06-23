{{ config(materialized='table') }}

SELECT
    id::TEXT AS sf_account_id,
    TRIM(name) AS name,
    TRIM(
        CASE WHEN billing_street IS NOT NULL THEN billing_street ELSE '' END || ', ' ||
        CASE WHEN billing_city IS NOT NULL THEN billing_city ELSE '' END || ', ' ||
        CASE WHEN billing_state IS NOT NULL THEN billing_state ELSE '' END || ', ' ||
        CASE WHEN billing_postal_code IS NOT NULL THEN billing_postal_code ELSE '' END || ', ' ||
        CASE WHEN billing_country IS NOT NULL THEN billing_country ELSE '' END
    ) AS address,
    CASE WHEN owner_id ~ '^\d+$' THEN owner_id::INTEGER ELSE NULL END AS owner_id,
    created_date::TIMESTAMP AS add_time
FROM {{ source('source_salesforce', 'account') }}
