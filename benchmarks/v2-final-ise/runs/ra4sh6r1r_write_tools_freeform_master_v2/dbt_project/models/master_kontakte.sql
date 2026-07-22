{{ config(materialized='table') }}

SELECT *
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
