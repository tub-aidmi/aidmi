-- depends_on: {{ ref('Account') }} -- placeholder for Account dependency, not for actual join in dbt model
-- depends_on: {{ ref('Opportunity') }} -- placeholder for Opportunity dependency, not for actual join in dbt model

{{ config(materialized='table') }}

WITH projects_with_related_ids AS (
    SELECT
        proj.proj_id,
        proj.name,
        proj.status,
        proj.go_live,
        proj.kd AS source_customer_id,
        proj.opp AS source_opportunity_id,
        kunden.kunden_nr,
        chancen.chance_id
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    LEFT JOIN
        {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
        ON proj.kd = kunden.kunden_nr
    LEFT JOIN
        {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
        ON proj.opp = chancen.chance_id
)
SELECT
    proj_id AS