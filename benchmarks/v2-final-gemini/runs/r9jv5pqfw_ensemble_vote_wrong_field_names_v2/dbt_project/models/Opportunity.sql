{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    COALESCE(chance.bezeichnung, 'Unknown Opportunity Name') AS "Name",
    COALESCE(chance.phase, 'Prospecting') AS "StageName",
    COALESCE(chance.abschlussdatum, CURRENT_DATE::TEXT) AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
