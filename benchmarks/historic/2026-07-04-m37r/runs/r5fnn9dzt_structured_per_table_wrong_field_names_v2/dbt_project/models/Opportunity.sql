-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    T1.chance_id AS "Id",
    COALESCE(T1.bezeichnung, 'Opportunity ' || T1.chance_id) AS "Name",
    CASE
        WHEN T1.phase = 'Prospecting' THEN 'Prospecting'
        WHEN T1.phase = 'Qualification' THEN 'Qualification'
        WHEN T1.phase = 'Closed Won' THEN 'Closed Won'
        WHEN T1.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for any unmapped phases as StageName is NOT NULL
    END AS "StageName",
    COALESCE(T1.abschlussdatum, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    T1.volumen AS "Amount",
    T1.waehrung AS "CurrencyIsoCode",
    T1.kd_nr AS "AccountId",
    T1.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS T1