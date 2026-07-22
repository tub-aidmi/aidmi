{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(c.phase) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(c.phase) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(c.phase) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(c.phase) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(c.phase) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(c.phase) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(c.phase) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(c.phase) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(c.phase) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(c.phase) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01'
    END AS "CloseDate",
    c.volumen AS "Amount",
    TRIM(UPPER(c.waehrung)) AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
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