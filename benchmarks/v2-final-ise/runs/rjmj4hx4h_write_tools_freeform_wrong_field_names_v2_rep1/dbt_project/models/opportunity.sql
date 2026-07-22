{{ config(materialized='table') }}

SELECT 
    SUBSTRING(MD5(chance_id), 1, 18) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    phase AS "StageName",
    CASE 
        WHEN abschlussdatum IS NOT NULL AND abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    '001' || SUBSTRING(MD5(kunden.kunden_nr), 1, 14) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
     '2024-01-01' AS "CreatedDate",
     '2024-01-01' AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} 
    ON chancen.kd_nr = kunden.kunden_nr
