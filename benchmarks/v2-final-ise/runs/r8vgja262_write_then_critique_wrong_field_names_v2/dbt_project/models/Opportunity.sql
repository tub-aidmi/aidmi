{{ config(materialized='table') }}
SELECT
    LOWER(MD5(c.chance_id)) AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN UPPER(TRIM(c.phase)) = 'PROSPEKT' THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) = 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'WERTVORSCHLAG' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) = 'ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) = 'WAHRNEHMUNG' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'ANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) = 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) = 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) = 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN c.volumen IS NOT NULL THEN c.volumen::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    LOWER(MD5(k.kunden_nr)) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr