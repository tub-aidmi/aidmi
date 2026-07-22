{{ config(materialized='table') }}

WITH opportunity_base AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON c.kd_nr = k.kunden_nr
)

SELECT
    MD5(chance_id) AS "Id",
    TRIM(bezeichnung) AS "Name",
    CASE
        WHEN UPPER(TRIM(phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(phase)) IN ('NEEDS ANALYSIS', 'BEDARF') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(phase)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(phase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(phase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(phase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(phase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(phase)) IN ('CLOSED WON', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(phase)) IN ('CLOSED LOST', 'VERLOREN') THEN 'Closed Lost'
        WHEN UPPER(TRIM(phase)) = 'INTERESSE' THEN 'Prospecting'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    TRIM(waehrung) AS "CurrencyIsoCode",
    MD5(kunden_nr) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_base