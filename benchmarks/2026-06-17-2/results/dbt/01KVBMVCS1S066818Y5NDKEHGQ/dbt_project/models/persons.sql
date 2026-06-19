{{ config(materialized='table') }}

SELECT
    COALESCE(c.id::text, 'UNKNOWN_PLACEHOLDER') AS sf_contact_id,
    TRIM(COALESCE(c.first_name || ' ', '') || COALESCE(c.last_name, '')) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE 
        WHEN c.owner_id IS NOT NULL AND c.owner_id ~ '^[0-9]+$' THEN c.owner_id::INTEGER 
        ELSE NULL 
    END AS owner_id,
    c.created_date AS add_time
FROM {{ source('src_01kvbmvcs1s066818y5ndkehgq_raw', 'contact') }} c
LEFT JOIN {{ source('src_01kvbmvcs1s066818y5ndkehgq_raw', 'account') }} a
    ON c.account_id = a.id