{{ config(materialized='table') }}

SELECT
    a.id AS sf_account_id,
    TRIM(a.name) AS name,
    TRIM(
        COALESCE(NULLIF(a.billing_street, ''), '') ||
        CASE WHEN a.billing_street IS NOT NULL AND TRIM(a.billing_street) != '' AND (a.billing_city IS NOT NULL OR a.billing_state IS NOT NULL OR a.billing_postal_code IS NOT NULL OR a.billing_country IS NOT NULL) THEN ', ' ELSE '' END ||
        COALESCE(NULLIF(a.billing_city, ''), '') ||
        CASE WHEN a.billing_city IS NOT NULL AND TRIM(a.billing_city) != '' AND (a.billing_state IS NOT NULL OR a.billing_postal_code IS NOT NULL OR a.billing_country IS NOT NULL) THEN ', ' ELSE '' END ||
        COALESCE(NULLIF(a.billing_state, ''), '') ||
        CASE WHEN a.billing_state IS NOT NULL AND TRIM(a.billing_state) != '' AND (a.billing_postal_code IS NOT NULL OR a.billing_country IS NOT NULL) THEN ' ' ELSE '' END ||
        COALESCE(NULLIF(a.billing_postal_code, ''), '') ||
        CASE WHEN a.billing_postal_code IS NOT NULL AND TRIM(a.billing_postal_code) != '' AND a.billing_country IS NOT NULL THEN ', ' ELSE '' END ||
        COALESCE(NULLIF(a.billing_country, ''), '')
    ) AS address,
    CASE
        WHEN a.owner_id ~ '^[0-9]+$' THEN CAST(a.owner_id AS INTEGER)
        ELSE NULL
    END AS owner_id,
    a.created_date AS add_time
FROM {{ source('src_01kvbnheswzcqbegpeeg5n4fm5_raw', 'account') }} a
WHERE a.is_deleted = FALSE OR a.is_deleted IS NULL