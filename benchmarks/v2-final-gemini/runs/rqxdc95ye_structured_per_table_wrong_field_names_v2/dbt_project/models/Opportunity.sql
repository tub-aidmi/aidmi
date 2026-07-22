-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN chancen.phase = 'Prospecting' THEN 'Prospecting'
        WHEN chancen.phase = 'Qualification' THEN 'Qualification'
        WHEN chancen.phase = 'Closed Won' THEN 'Closed Won'
        WHEN chancen.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        '1900-01-01' -- Default date for NOT NULL
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    '1900-01-01' AS "CreatedDate", -- Default as no source column is available
    '1900-01-01' AS "LastModifiedDate", -- Default as no source column is available
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen