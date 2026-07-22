{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    bezeichnung AS "Name",
    CASE
        WHEN phase = 'Prospecting' THEN 'Prospecting'
        WHEN phase = 'Qualification' THEN 'Qualification'
        WHEN phase = 'Closed Lost' THEN 'Closed Lost'
        WHEN phase = 'Closed Won' THEN 'Closed Won'
        ELSE NULL -- Default or handle other phases if any
    END AS "StageName",
    abschlussdatum::text AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
