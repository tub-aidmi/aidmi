{{ config(materialized='table') }}

WITH accounts AS (
    SELECT
        id,
        TRIM(name) AS org_name
    FROM {{ source('src_01kvb5z64zpt5rg6x5bkxq7971_raw', 'account') }}
),

contacts AS (
    SELECT
        id AS sf_contact_id,
        TRIM(CONCAT_WS(' ', first_name, last_name)) AS name,
        LOWER(TRIM(email)) AS email,
        COALESCE(phone, mobile_phone) AS phone,
        account_id,
        owner_id,
        created_date AS add_time
    FROM {{ source('src_01kvb5z64zpt5rg6x5bkxq7971_raw', 'contact') }}
)

SELECT
    c.sf_contact_id,
    c.name,
    c.email,
    c.phone,
    a.org_name,
    CASE 
        WHEN c.owner_id ~ '^[0-9]+$' THEN CAST(c.owner_id AS INTEGER) 
        ELSE NULL 
    END AS owner_id,
    c.add_time
FROM contacts c
LEFT JOIN accounts a ON c.account_id = a.id