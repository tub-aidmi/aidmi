{{ config(materialized='table') }}

SELECT 
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE 
        WHEN c.phase ILIKE '%prospekt%' THEN 'Prospecting'
        WHEN c.phase ILIKE '%qualif%' THEN 'Qualification'
        WHEN c.phase ILIKE '%bedarf%' OR c.phase ILIKE '%needs%' THEN 'Needs Analysis'
        WHEN c.phase ILIKE '%wert%' OR c.phase ILIKE '%value%' THEN 'Value Proposition'
        WHEN c.phase ILIKE '%entscheider%' OR c.phase ILIKE '%decision%' THEN 'Id. Decision Makers'
        WHEN c.phase ILIKE '%wahrnehmung%' OR c.phase ILIKE '%perception%' THEN 'Perception Analysis'
        WHEN c.phase ILIKE '%angebot%' OR c.phase ILIKE '%proposal%' OR c.phase ILIKE '%quote%' THEN 'Proposal/Price Quote'
        WHEN c.phase ILIKE '%verhandlung%' OR c.phase ILIKE '%negotiation%' THEN 'Negotiation/Review'
        WHEN c.phase ILIKE '%gewonnen%' OR c.phase ILIKE '%won%' THEN 'Closed Won'
        WHEN c.phase ILIKE '%verloren%' OR c.phase ILIKE '%lost%' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr