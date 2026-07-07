{{ config(materialized='table') }}

SELECT
    *
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}