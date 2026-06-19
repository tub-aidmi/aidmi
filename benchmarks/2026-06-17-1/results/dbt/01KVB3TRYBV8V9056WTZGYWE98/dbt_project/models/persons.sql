{{ config(materialized='table') }}

SELECT
    c.id AS sf_contact_id,
    TRIM(CONCAT_WS(' ', c.first_name, c.last_name)) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE 
        WHEN c.owner_id ~ '^[0-9]+$' THEN c.owner_id::integer 
        ELSE NULL 
    END AS owner_id,
    c.created_date AS add_time
FROM {{ source('src_01kvb3trybv8v9056wtzgywe98_raw', 'contact') }} AS c
LEFT JOIN {{ source('src_01kvb3trybv8v9056wtzgywe98_raw', 'account') }} AS a
    ON c.account_id = a.id