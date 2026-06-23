{{ config(materialized='table') }}

SELECT
    id AS sf_account_id,
    TRIM(name) AS name,
    CONCAT_WS(', ', billing_street, billing_city, billing_state, billing_postal_code, billing_country) AS address,
    CAST(owner_id AS INTEGER) AS owner_id,
    created_date AS add_time
FROM {{ source('src_01kvbmep7nfdx9h100wxm6fqh6_raw', 'account') }}