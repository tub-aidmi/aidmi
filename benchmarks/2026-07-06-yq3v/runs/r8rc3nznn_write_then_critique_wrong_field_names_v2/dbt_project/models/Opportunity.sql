-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
{{ config(materialized='table') }}

SELECT
    MD5(chance_id) AS "Id",
    COALESCE(bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(phase)) IN ('prospecting', 'anbahnung') THEN 'Prospecting'
        WHEN LOWER(TRIM(phase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(phase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) IN ('id. decision makers', 'entscheider identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) IN ('closed won', 'gewonnen', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) IN ('closed lost', 'verloren', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN TRIM(abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(abschlussdatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01'
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    MD5(kd_nr) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}