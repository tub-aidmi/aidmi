{% set src_account = source('src_01kvbnkyxqz1yws1pp56ygkg1w_raw', 'account') %}
{% set src_contact = source('src_01kvbnkyxqz1yws1pp56ygkg1w_raw', 'contact') %}
{{ config(materialized='table') }}
SELECT
    c.id AS sf_contact_id,
    TRIM(
        CASE
            WHEN c.first_name IS NOT NULL AND c.last_name IS NOT NULL THEN
                c.first_name || ' ' || c.last_name
            ELSE
                COALESCE(c.name, '')
        END
    ) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(TRIM(c.phone), TRIM(c.mobile_phone)) AS phone,
    TRIM(a.name) AS org_name,
    CASE
        WHEN c.owner_id ~ '^\d+$' THEN CAST(c.owner_id AS INTEGER)
        ELSE NULL
    END AS owner_id,
    c.created_date AS add_time
FROM {{ src_contact }} AS c
LEFT JOIN {{ src_account }} AS a
    ON c.account_id = a.id
WHERE c.is_deleted = FALSE;