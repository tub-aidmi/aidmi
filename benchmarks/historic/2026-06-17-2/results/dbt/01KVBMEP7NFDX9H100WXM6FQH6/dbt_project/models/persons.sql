{{ config(materialized='table') }}

SELECT 
    c.id AS sf_contact_id,
    TRIM(c.first_name || ' ' || c.last_name) AS name,
    TRIM(LOWER(c.email)) AS email,
    COALESCE(TRIM(c.phone), TRIM(c.mobile_phone)) AS phone,
    a.name AS org_name,
    CASE WHEN c.owner_id ~ '^[0-9]+$' THEN c.owner_id::integer ELSE NULL END AS owner_id,
    c.created_date AS add_time
FROM {{ source('src_01kvbmep7nfdx9h100wxm6fqh6_raw', 'contact') }} c
LEFT JOIN {{ source('src_01kvbmep7nfdx9h100wxm6fqh6_raw', 'account') }} a 
    ON c.account_id = a.id
WHERE c.is_deleted IS NOT TRUE