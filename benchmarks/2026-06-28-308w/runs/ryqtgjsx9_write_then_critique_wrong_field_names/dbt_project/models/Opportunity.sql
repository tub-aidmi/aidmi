
{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Opportunity ' || chancen.chance_id) AS "Name",
    CASE
        WHEN chancen.phase = 'Prospecting' THEN 'Prospecting'
        WHEN chancen.phase = 'Qualification' THEN 'Qualification'
        WHEN chancen.phase = 'Closed Lost' THEN 'Closed Lost'
        WHEN chancen.phase = 'Closed Won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default for NULL or unmapped phases to satisfy NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN chancen.abschlussdatum
            ELSE NULL
        END,
        CURRENT_DATE::TEXT
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'chancen') }} AS chancen
