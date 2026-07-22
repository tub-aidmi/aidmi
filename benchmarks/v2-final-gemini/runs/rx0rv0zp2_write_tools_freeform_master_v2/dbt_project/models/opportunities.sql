{{ config(materialized='table') }}

SELECT
    opps.opp_kennung AS "Id"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opps
