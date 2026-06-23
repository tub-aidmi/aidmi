{{ config(materialized='table') }}

SELECT
    a.id AS sf_account_id,
    TRIM(a.name) AS name,
    CASE
        WHEN a.billing_street IS NOT NULL OR a.billing_city IS NOT NULL OR a.billing_state IS NOT NULL OR a.billing_postal_code IS NOT NULL OR a.billing_country IS NOT NULL
        THEN TRIM(
            CONCAT_WS(', ',
                NULLIF(TRIM(a.billing_street), ''),
                NULLIF(TRIM(a.billing_city), ''),
                NULLIF(TRIM(a.billing_state), ''),
                NULLIF(TRIM(a.billing_postal_code), ''),
                NULLIF(TRIM(a.billing_country), '')
            )
        )
        ELSE NULL
    END AS address,
    CASE
        WHEN a.owner_id ~ '^[0-9]+$' THEN CAST(a.owner_id AS INTEGER)
        ELSE NULL
    END AS owner_id,
    a.created_date AS add_time
FROM {{ source('src_01kvbn9rgjagq13rgkx04s096v_raw', 'account') }} a
WHERE a.is_deleted = FALSE OR a.is_deleted IS NULL