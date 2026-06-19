{{ config(materialized='table') }}

SELECT
    c.id AS sf_contact_id,
    CASE 
        WHEN c.first_name IS NOT NULL OR c.last_name IS NOT NULL
        THEN TRIM(
            COALESCE(c.first_name, '') || 
            CASE WHEN c.first_name IS NOT NULL AND c.last_name IS NOT NULL THEN ' ' ELSE '' END ||
            COALESCE(c.last_name, '')
        )
        ELSE c.name
    END AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE WHEN c.owner_id ~ '^\d+$' THEN c.owner_id::INTEGER ELSE NULL END AS owner_id,
    c.created_date AS add_time
FROM {{ source('salesforce', 'contact') }} c
LEFT JOIN {{ source('salesforce', 'account') }} a ON c.account_id = a.id
