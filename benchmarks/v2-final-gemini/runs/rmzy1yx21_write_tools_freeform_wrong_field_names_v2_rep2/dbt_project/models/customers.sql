{{ config(materialized='table') }}

SELECT
    Id AS "Id",
    Name AS "Name"
FROM {{ source('salesforce', 'account') }}
