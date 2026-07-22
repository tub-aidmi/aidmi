-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    o.chance_id AS "Id",
    COALESCE(o.bezeichnung, 'Unnamed Opportunity') AS "Name",
    COALESCE(o.phase, 'Prospecting') AS "StageName",
    COALESCE(TO_CHAR(TO_DATE(o.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'), '1900-01-01') AS "CloseDate",
    o.volumen AS "Amount",
    o.waehrung AS "CurrencyIsoCode",
    o.kd_nr AS "AccountId",
    o.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS o