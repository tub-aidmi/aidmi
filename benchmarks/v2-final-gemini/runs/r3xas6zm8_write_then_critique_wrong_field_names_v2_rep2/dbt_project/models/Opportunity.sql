-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}

{{ config(materialized='table') }}

WITH cleaned_chancen AS (
    SELECT
        TRIM(chance_id) AS chance_id,
        TRIM(bezeichnung) AS bezeichnung,
        TRIM(phase) AS phase,
        NULLIF(TRIM(abschlussdatum), '') AS abschlussdatum,
        volumen AS volumen,
        TRIM(waehrung) AS waehrung,
        TRIM(kd_nr) AS kd_nr
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
),
cleaned_kunden AS (
    SELECT
        TRIM(kunden_nr) AS kunden_nr
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, 'Unknown Opportunity ' || c.chance_id) AS "Name",
    CASE
        WHEN TRIM(c.phase) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(c.phase) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(c.phase) = 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(c.phase) = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL -- Fallback to NULL as requested by reviewer for unparseable/missing dates
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_chancen AS c
LEFT JOIN
    cleaned_kunden AS k
ON
    c.kd_nr = k.kunden_nr