{{ config(materialized='table') }}

SELECT
    proj.projekt_kennung AS "Id"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS proj
