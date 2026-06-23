{{ config(materialized='table') }}

SELECT
    c.id AS sf_contact_id,
    TRIM(
        COALESCE(NULLIF(c.first_name, ''), '') || ' ' ||
        COALESCE(NULLIF(c.last_name, ''), '')
    ) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(
        NULLIF(TRIM(c.phone), ''),
        NULLIF(TRIM(c.mobile_phone), '')
    ) AS phone,
    TRIM(a.name) AS org_name,
    CASE
        WHEN c.owner_id ~ '^[0-9]+$' THEN CAST(c.owner_id AS INTEGER)
        ELSE NULL
    END AS owner_id,
    c.created_date AS add_time
FROM {{ source('src_01kvbn9rgjagq13rgkx04s096v_raw', 'contact') }} c
LEFT JOIN {{ source('src_01kvbn9rgjagq13rgkx04s096v_raw', 'account') }} a
    ON c.account_id = a.id
WHERE c.is_deleted = FALSE OR c.is_deleted IS NULL