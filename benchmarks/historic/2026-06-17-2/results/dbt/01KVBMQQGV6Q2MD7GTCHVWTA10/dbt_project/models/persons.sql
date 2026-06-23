{{ config(materialized='table') }}

SELECT
    c.id::TEXT AS sf_contact_id,
    TRIM(c.first_name || ' ' || c.last_name) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE WHEN c.owner_id ~ '^\d+$' THEN c.owner_id::INTEGER ELSE NULL END AS owner_id,
    c.created_date::TIMESTAMP AS add_time
FROM {{ source('source_salesforce', 'contact') }} c
LEFT JOIN {{ source('source_salesforce', 'account') }} a
    ON c.account_id = a.id
