-- models/Opportunity.sql
{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN LOWER(c.phase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(c.phase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(c.phase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(c.phase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(c.phase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(c.phase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(c.phase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(c.phase) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(c.phase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(c.phase) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    -- Date parsing for CloseDate
    CASE
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(c.abschlussdatum, 'DD.MM.YYYY')::TEXT
        WHEN c.abschlussdatum ~ '^\d{8}$' THEN TO_DATE(c.abschlussdatum, 'YYYYMMDD')::TEXT
        WHEN c.abschlussdatum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_DATE(c.abschlussdatum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
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
