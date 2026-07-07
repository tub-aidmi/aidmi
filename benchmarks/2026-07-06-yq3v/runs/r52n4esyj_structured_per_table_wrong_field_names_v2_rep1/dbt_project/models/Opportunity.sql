{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    chance.bezeichnung AS "Name",
    chance.phase AS "StageName",
    chance.abschlussdatum AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
