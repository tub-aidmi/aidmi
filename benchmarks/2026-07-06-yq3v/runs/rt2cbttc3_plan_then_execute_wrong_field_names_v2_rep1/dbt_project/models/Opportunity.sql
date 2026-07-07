{{ config(materialized='table') }}

SELECT
    ch.chance_id AS "Id",
    COALESCE(ch.bezeichnung, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN ch.phase = 'Prospecting' THEN 'Prospecting'
        WHEN ch.phase = 'Qualification' THEN 'Qualification'
        WHEN ch.phase = 'Closed Won' THEN 'Closed Won'
        WHEN ch.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(ch.abschlussdatum, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    ch.volumen AS "Amount",
    ch.waehrung AS "CurrencyIsoCode",
    ch.kd_nr AS "AccountId",
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS ch
