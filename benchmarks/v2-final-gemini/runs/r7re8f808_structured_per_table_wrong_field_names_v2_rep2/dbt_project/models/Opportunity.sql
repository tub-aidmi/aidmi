-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    TRIM(c.bezeichnung) AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) = 'angebotsphase' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) = 'verhandlung' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c.phase)) IN ('abgeschlossen gewonnen', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) IN ('abgeschlossen verloren', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(TRIM(c.phase)) = 'bedürfnisanalyse' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) = 'wertvorschlag' THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) = 'identifizierung entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('interesse', 'erste kontaktaufnahme', 'prospektierung') THEN 'Prospecting'
        ELSE NULL -- Fallback for unmapped phases
    END AS "StageName",
    -- Date parsing for Abschlussdatum
    CASE
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum -- YYYY-MM-DD
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    c.kd_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c