{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    chance.bezeichnung AS "Name",
    -- StageName mapping
    CASE
        WHEN LOWER(TRIM(chance.phase)) IN ('prospecting', 'qualifikation', 'initial contact') THEN 'Prospecting'
        WHEN LOWER(TRIM(chance.phase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(chance.phase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(chance.phase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(chance.phase)) IN ('id. decision makers', 'entscheider identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(chance.phase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(chance.phase)) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(chance.phase)) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(chance.phase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(chance.phase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    -- CloseDate parsing and formatting (YYYY-MM-DD)
    COALESCE(
        TO_CHAR(TO_DATE(chance.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chance.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chance.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        CURRENT_DATE::TEXT -- Fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId", -- Assuming kd_nr directly maps to Account.Id
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No direct source
    NULL AS "LastModifiedDate", -- No direct source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
