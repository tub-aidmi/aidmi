{{ config(materialized='table') }}

SELECT
    MD5(c.chance_id) AS "Id",
    COALESCE(c.bezeichnung, 'No Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'), CURRENT_DATE::TEXT) AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    MD5(k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr
