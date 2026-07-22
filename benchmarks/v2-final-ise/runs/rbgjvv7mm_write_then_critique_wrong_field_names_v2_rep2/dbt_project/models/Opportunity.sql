{{ config(materialized='table') }}
SELECT
    'opp_' || c.chance_id AS "Id",
    INITCAP(TRIM(c.bezeichnung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) IN ('prospektierung', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) IN ('qualifikation', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) IN ('bedarfsanalyse', 'needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('wertversprechen', 'value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) IN ('entscheidungsträger identifiziert', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('angebot/preisangebot', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) IN ('verhandlung/prüfung', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c.phase)) IN ('abgeschlossen gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) IN ('abgeschlossen verloren', 'closed lost') THEN 'Closed Lost'
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
    INITCAP(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    md5('acc_' || k.kunden_nr)::uuid::text AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr