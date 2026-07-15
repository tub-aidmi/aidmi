{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(phase)) IN ('PROSPECTING', 'QUALIFICATION', 'NEEDS ANALYSIS', 'VALUE PROPOSITION', 'ID. DECISION MAKERS', 'PERCEPTION ANALYSIS', 'PROPOSAL/PRICE QUOTE', 'NEGOTIATION/REVIEW', 'CLOSED WON', 'CLOSED LOST') THEN INITCAP(LOWER(TRIM(phase)))
        WHEN UPPER(TRIM(phase)) = 'ANBAHNUNG' THEN 'Prospecting'
        WHEN UPPER(TRIM(phase)) = 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN UPPER(TRIM(phase)) = 'ANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(phase)) = 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(phase)) = 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(phase)) = 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    CASE
        WHEN UPPER(TRIM(waehrung)) IN ('EUR', 'USD', 'GBP', 'CHF', 'JPY') THEN UPPER(TRIM(waehrung))
        ELSE NULL
    END AS "CurrencyIsoCode",
    kunden.kunden_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden ON chancen.kd_nr = kunden.kunden_nr