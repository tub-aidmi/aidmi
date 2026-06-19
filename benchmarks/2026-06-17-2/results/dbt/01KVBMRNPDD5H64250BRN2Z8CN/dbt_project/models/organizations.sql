{{ config(materialized='table') }}

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CASE 
        WHEN billing_street IS NOT NULL OR billing_city IS NOT NULL OR billing_state IS NOT NULL OR billing_postal_code IS NOT NULL OR billing_country IS NOT NULL
        THEN TRIM(
            COALESCE(billing_street, '') || 
            CASE WHEN billing_street IS NOT NULL AND (billing_city IS NOT NULL OR billing_state IS NOT NULL OR billing_postal_code IS NOT NULL OR billing_country IS NOT NULL) THEN ', ' ELSE '' END ||
            COALESCE(billing_city, '') || 
            CASE WHEN billing_city IS NOT NULL AND (billing_state IS NOT NULL OR billing_postal_code IS NOT NULL OR billing_country IS NOT NULL) THEN ', ' ELSE '' END ||
            COALESCE(billing_state, '') || 
            CASE WHEN billing_state IS NOT NULL AND (billing_postal_code IS NOT NULL OR billing_country IS NOT NULL) THEN ' ' ELSE '' END ||
            COALESCE(billing_postal_code, '') || 
            CASE WHEN (billing_street IS NOT NULL OR billing_city IS NOT NULL OR billing_state IS NOT NULL OR billing_postal_code IS NOT NULL) AND billing_country IS NOT NULL THEN ', ' ELSE '' END ||
            COALESCE(billing_country, '')
        )
        ELSE NULL
    END AS address,
    CASE WHEN owner_id ~ '^\d+$' THEN owner_id::INTEGER ELSE NULL END AS owner_id,
    created_date AS add_time
FROM {{ source('salesforce', 'account') }}
