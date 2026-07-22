{{ config(materialized='table') }}

SELECT
    MD5(chance_id) AS "Id",
    TRIM(bezeichnung) AS "Name",
    CASE 
        WHEN LOWER(phase) IN ('prospektierung', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(phase) IN ('qualifikation', 'qualification') THEN 'Qualification'
        WHEN LOWER(phase) IN ('bedarfsanalyse', 'needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(phase) IN ('wertangebot', 'value proposition') THEN 'Value Proposition'
        WHEN LOWER(phase) IN ('entscheider identifiziert', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(phase) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(phase) IN ('angebot/preis', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(phase) IN ('verhandlung', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(phase) IN ('gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(phase) IN ('verloren', 'closed lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    MD5(kd_nr) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
