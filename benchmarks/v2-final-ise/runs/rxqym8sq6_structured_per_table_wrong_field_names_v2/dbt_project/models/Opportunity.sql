{{ config(materialized='table') }}

SELECT
    CONCAT('006', LEFT(MD5(c.chance_id), 15)) AS "Id",
    TRIM(c.bezeichnung) AS "Name",
    c.phase AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    k.erp_nummer AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
INNER JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON c.kd_nr = k.kunden_nr