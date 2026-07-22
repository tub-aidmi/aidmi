{{ config(materialized='table') }}

SELECT
    'Opportunity_' || c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN UPPER(TRIM(c.phase)) = 'PROSPEKTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) = 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) = 'VERHANDLUNG/PRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) = 'VERLOREN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    CASE
        WHEN UPPER(TRIM(c.waehrung)) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(c.waehrung)) IN ('USD', '$') THEN 'USD'
        WHEN UPPER(TRIM(c.waehrung)) IN ('CHF', 'SFR') THEN 'CHF'
        WHEN UPPER(TRIM(c.waehrung)) IN ('GBP', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    'Account_' || k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON c.kd_nr = k.kunden_nr