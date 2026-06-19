{{ config(materialized='table') }}

WITH raw_contacts AS (
    SELECT * FROM {{ source('salesforce', 'contact') }}
),
raw_accounts AS (
    SELECT id, name FROM {{ source('salesforce', 'account') }}
)

SELECT
    c.id AS sf_contact_id,
    TRIM(CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, ''))) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE 
        WHEN c.owner_id ~ '^\d+$' THEN c.owner_id::INTEGER 
        ELSE NULL 
    END AS owner_id,
    c.created_date AS add_time
FROM raw_contacts c
LEFT JOIN raw_accounts a ON c.account_id = a.id
