{{ config(materialized='table') }}
SELECT
    CONCAT('001', c.chance_id) AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    CONCAT('001', k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr