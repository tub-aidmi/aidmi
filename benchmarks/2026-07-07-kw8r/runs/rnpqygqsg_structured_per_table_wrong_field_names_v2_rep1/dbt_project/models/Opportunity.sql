{{ config(materialized='table') }}

SELECT
    c."chance_id" AS "Id",
    c."bezeichnung" AS "Name",
    c."phase" AS "StageName",
    c."abschlussdatum" AS "CloseDate",
    c."volumen" AS "Amount",
    c."waehrung" AS "CurrencyIsoCode",
    c."kd_nr" AS "AccountId",
    c."chance_id" AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c