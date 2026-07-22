-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    COALESCE(c.phase, 'Prospecting') AS "StageName",
    COALESCE(c.abschlussdatum, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    c.kd_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c