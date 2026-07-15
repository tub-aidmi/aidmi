{{ config(materialized='table') }}
SELECT
    c.chance_id AS "Id",
    TRIM(c.bezeichnung) AS "Name",
    CASE
        WHEN UPPER(TRIM(c.phase)) = 'AUSSICHT' THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) = 'ENTSCHEIDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) = 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) = 'VERLOREN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    TRIM(UPPER(c.waehrung)) AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)