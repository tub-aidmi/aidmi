{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT
        id AS sf_contact_id,
        TRIM(CONCAT_WS(' ', first_name, last_name)) AS name,
        LOWER(TRIM(email)) AS email,
        COALESCE(TRIM(phone), TRIM(mobile_phone)) AS phone,
        account_id,
        created_date AS add_time,
        owner_id
    FROM {{ source('src_01kvbmep7hgvp2fzyh4kevs4j2_raw', 'contact') }}
    WHERE id IS NOT NULL
),

account_data AS (
    SELECT
        id,
        TRIM(name) AS org_name,
        owner_id
    FROM {{ source('src_01kvbmep7hgvp2fzyh4kevs4j2_raw', 'account') }}
    WHERE id IS NOT NULL
)

SELECT
    c.sf_contact_id,
    c.name,
    c.email,
    c.phone,
    a.org_name,
    CASE WHEN c.owner_id ~ '^[0-9]+$' THEN c.owner_id::integer ELSE NULL END AS owner_id,
    c.add_time
FROM contact_data c
LEFT JOIN account_data a ON c.account_id = a.id