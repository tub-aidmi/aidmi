{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN chancen.phase = 'Prospecting' THEN 'Prospecting'
        WHEN chancen.phase = 'Qualification' THEN 'Qualification'
        WHEN chancen.phase = 'Closed Won' THEN 'Closed Won'
        WHEN chancen.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NULL or unknown phases
    END AS "StageName",
    COALESCE(chancen.abschlussdatum, CURRENT_DATE::TEXT) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen