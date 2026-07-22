{{ config(materialized='table') }}

SELECT
    kunden.kundennummer AS "Id"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
