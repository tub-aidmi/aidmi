{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN chancen.phase = 'Prospecting' THEN 'Prospecting'
        WHEN chancen.phase = 'Qualification' THEN 'Qualification'
        WHEN chancen.phase = 'Closed Lost' THEN 'Closed Lost'
        WHEN chancen.phase = 'Closed Won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default for unknown phases as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        (CASE WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(chancen.abschlussdatum::DATE, 'YYYY-MM-DD') ELSE NULL END),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
