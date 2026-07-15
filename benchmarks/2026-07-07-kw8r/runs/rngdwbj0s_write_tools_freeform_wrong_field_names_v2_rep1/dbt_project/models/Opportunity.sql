{{ config(materialized='table') }}

SELECT
    'OPP_' || c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN UPPER(TRIM(c.phase)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) IN ('QUALIFICATION', 'QUALIFIZIERUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) IN ('VALUE PROPOSITION', 'WERTVORLAGE') THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) IN ('CLOSED WON', 'GESCHLOSSEN GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) IN ('CLOSED LOST', 'GESCHLOSSEN VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    'ACCT_' || k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr
