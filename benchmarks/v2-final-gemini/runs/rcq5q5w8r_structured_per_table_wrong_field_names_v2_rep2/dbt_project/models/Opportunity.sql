-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(c.phase) = 'prospektion' THEN 'Prospecting'
        WHEN LOWER(c.phase) = 'qualifikation' THEN 'Qualification'
        WHEN LOWER(c.phase) = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN LOWER(c.phase) = 'wertangebot' THEN 'Value Proposition'
        WHEN LOWER(c.phase) = 'entscheidungsträger identifiziert' THEN 'Id. Decision Makers'
        WHEN LOWER(c.phase) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(c.phase) = 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN LOWER(c.phase) = 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(c.phase) = 'gewonnen' THEN 'Closed Won'
        WHEN LOWER(c.phase) = 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default stage for unmapped or NULL values
    END AS "StageName",
    COALESCE(TO_CHAR(CAST(c.abschlussdatum AS DATE), 'YYYY-MM-DD'), TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(c.waehrung) AS "CurrencyIsoCode",
    c.kd_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c