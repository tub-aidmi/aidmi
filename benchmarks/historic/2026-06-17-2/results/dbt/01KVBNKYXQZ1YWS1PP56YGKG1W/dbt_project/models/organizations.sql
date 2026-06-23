{% set src = source('src_01kvbnkyxqz1yws1pp56ygkg1w_raw', 'account') %}
{{ config(materialized='table') }}
SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(', ',
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country
    ) AS address,
    CASE
        WHEN owner_id ~ '^\d+$' THEN owner_id::INTEGER
        ELSE NULL
    END AS owner_id,
    created_date AS add_time
FROM {{ src }}
WHERE is_deleted = FALSE;