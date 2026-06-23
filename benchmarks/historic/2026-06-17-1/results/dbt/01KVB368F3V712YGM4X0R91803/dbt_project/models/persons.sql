{{ config(materialized='table') }}

SELECT
    c.id AS sf_contact_id,
    TRIM(CONCAT_WS(' ', c.first_name, c.last_name)) AS name,
    LOWER(TRIM(c.email)) AS email,
    COALESCE(c.phone, c.mobile_phone) AS phone,
    a.name AS org_name,
    CASE
        WHEN c.owner_id ~ '^[0-9]+$' THEN CAST(c.owner_id AS INTEGER)
        ELSE NULL
    END AS owner_id,
    c.created_date AS add_time
FROM {{ source('src_01kvb368f3v712ygm4x0r91803_raw', 'contact') }} AS c
LEFT JOIN {{ source('src_01kvb368f3v712ygm4x0r91803_raw', 'account') }} AS a
    ON c.account_id = a.id