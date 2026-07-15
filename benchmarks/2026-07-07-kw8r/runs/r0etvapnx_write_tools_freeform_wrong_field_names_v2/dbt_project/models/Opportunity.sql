{{ config(materialized='table') }}

SELECT 
    '001' || RIGHT('000000' || REGEXP_REPLACE(ch.kd_nr, '[^0-9]', '', 'g'), 6) AS "Id",
    ch.bezeichnung AS "Name",
    CASE 
        WHEN TRIM(LOWER(ch.phase)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(ch.phase)) = 'closed lost' THEN 'Closed Lost'
        WHEN TRIM(LOWER(ch.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(ch.phase)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(ch.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(ch.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(ch.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(ch.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(ch.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(ch.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN ch.abschlussdatum IS NOT NULL AND ch.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN ch.abschlussdatum
        WHEN ch.abschlussdatum IS NOT NULL AND ch.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ch.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ch.abschlussdatum IS NOT NULL AND ch.abschlussdatum ~ '^\d{8}$' THEN SUBSTR(ch.abschlussdatum, 1, 4) || '-' || SUBSTR(ch.abschlussdatum, 5, 2) || '-' || SUBSTR(ch.abschlussdatum, 7, 2)
        ELSE NULL
    END AS "CloseDate",
    ch.volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    '001' || RIGHT('000000' || REGEXP_REPLACE(ch.kd_nr, '[^0-9]', '', 'g'), 6) AS "AccountId",
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} ch
