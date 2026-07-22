{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    bezeichnung AS "Name",
    CASE
        WHEN UPPER(phase) IN ('PROSPECTING', 'QUALIFICATION', 'NEEDS ANALYSIS', 'VALUE PROPOSITION', 'ID. DECISION MAKERS', 'PERCEPTION ANALYSIS', 'PROPOSAL/PRICE QUOTE', 'NEGOTIATION/REVIEW', 'CLOSED WON', 'CLOSED LOST') THEN INITCAP(LOWER(phase))
        WHEN UPPER(phase) IN ('AKQUISE', 'QUALIFIZIERUNG') THEN 'Qualification'
        WHEN UPPER(phase) IN ('BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(phase) IN ('ANGEBOT', 'ANGEBOTSLEGUNG') THEN 'Proposal/Price Quote'
        WHEN UPPER(phase) IN ('VERHANDLUNG') THEN 'Negotiation/Review'
        WHEN UPPER(phase) IN ('GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(phase) IN ('VERLOREN') THEN 'Closed Lost'
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
    k.kunden_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr
