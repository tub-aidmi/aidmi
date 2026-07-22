{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(TRIM(chancen.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(chancen.phase)) IN ('prospecting', 'neu') THEN 'Prospecting'
        WHEN LOWER(TRIM(chancen.phase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(chancen.phase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(chancen.phase)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(chancen.phase)) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(chancen.phase)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(chancen.phase)) IN ('proposal/price quote', 'angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(chancen.phase)) IN ('negotiation/review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(chancen.phase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(chancen.phase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    MD5(kunden.kunden_nr) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr