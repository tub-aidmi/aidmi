{{ config(materialized='table') }}

SELECT 
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE 
        WHEN c.phase = 'Prospecting' THEN 'Prospecting'
        WHEN c.phase = 'Qualification' THEN 'Qualification'
        WHEN c.phase = 'Closed Won' THEN 'Closed Won'
        WHEN c.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    c.abschlussdatum AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON c.kd_nr = k.kunden_nr