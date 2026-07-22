{{ config(materialized='table') }}

SELECT
    *
FROM
    {{ source('my_source', 'accounts_source') }}
