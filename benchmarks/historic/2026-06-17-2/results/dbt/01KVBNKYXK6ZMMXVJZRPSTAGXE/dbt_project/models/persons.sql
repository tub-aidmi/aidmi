{{ config(materialized='table') }}

SELECT 
    c.id AS sf_contact_id,
    CONCAT_WS(' ', c.first_name, c.last_name) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    TRIM(a.name) AS org_name,
    CASE 
        WHEN c.owner_id IS NOT NULL AND c.owner_id ~ '^\d+$' THEN CAST(TRIM(c.owner_id) AS INTEGER)
        ELSE NULL 
    END AS owner_id,
    c.created_date::timestamp AS add_time
FROM {{ source('src_01kvbnkyxk6zmmxvjzrpstagxe_raw', 'contact') }} c
LEFT JOIN {{ source('src_01kvbnkyxk6zmmxvjzrpstagxe_raw', 'account') }} a 
    ON c.account_id = a.id
WHERE LOWER(TRIM(c.email)) IS NOT NULL 
  AND LOWER(TRIM(c.email)) != ''