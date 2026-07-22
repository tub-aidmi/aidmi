{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) IN ('prospecting', 'initial contact', 'lead', 'angebotsphase') THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('value proposition', 'wertvorschlag') THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) IN ('id. decision makers', 'entscheideridentifikation') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('proposal/price quote', 'angebot/preis') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c.phase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '1900-01-01' -- Default for NOT NULL, if parsing fails
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr
