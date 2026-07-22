{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    TRIM(bezeichnung) AS "Name",
    CASE 
        WHEN LOWER(TRIM(phase)) IN ('prospektierung', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(phase)) IN ('qualifikation', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(phase)) IN ('bedarfsanalyse', 'needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) IN ('wertversprechen', 'value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) IN ('entscheider identifiziert', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) IN ('angebot/preisangebot', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) IN ('verhandlung/überprüfung', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) IN ('geschlossen gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) IN ('geschlossen verloren', 'closed lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
