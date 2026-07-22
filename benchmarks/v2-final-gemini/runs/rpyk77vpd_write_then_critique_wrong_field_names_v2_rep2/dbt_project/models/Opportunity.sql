-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

WITH source_chancen AS (
    SELECT
        chance_id,
        bezeichnung,
        phase,
        abschlussdatum,
        volumen,
        waehrung,
        kd_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
),

source_kunden AS (
    SELECT
        kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    s_chancen.chance_id AS "Id",
    s_chancen.bezeichnung AS "Name",
    COALESCE(s_chancen.phase, 'Prospecting') AS "StageName", -- Default to 'Prospecting' if source phase is NULL
    s_chancen.abschlussdatum AS "CloseDate",
    s_chancen.volumen AS "Amount",
    s_chancen.waehrung AS "CurrencyIsoCode",
    s_kunden.kunden_nr AS "AccountId",
    s_chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_chancen AS s_chancen
LEFT JOIN
    source_kunden AS s_kunden
ON
    s_chancen.kd_nr = s_kunden.kunden_nr