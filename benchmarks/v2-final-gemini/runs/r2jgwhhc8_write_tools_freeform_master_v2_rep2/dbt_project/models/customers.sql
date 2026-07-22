-- models/customers.sql
{{ config(materialized='table') }}

SELECT
    id AS "CustomerID",
    name AS "CustomerName",
    email AS "CustomerEmail"
FROM {{ source('raw_data', 'customers') }}
