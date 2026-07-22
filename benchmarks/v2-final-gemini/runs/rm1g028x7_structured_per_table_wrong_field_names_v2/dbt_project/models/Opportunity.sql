{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        TRIM(c.chance_id) AS opportunity_id,
        TRIM(c.bezeichnung) AS opportunity_name,
        LOWER(TRIM(c.phase)) AS opportunity_phase,
        TRIM(c.abschlussdatum) AS opportunity_close_date_raw,
        c.volumen AS opportunity_amount,
        TRIM(c.waehrung) AS opportunity_currency,
        TRIM(c.kd_nr) AS customer_number
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
)
SELECT
    opportunity_id AS "Id",
    COALESCE(opportunity_name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN opportunity_phase = 'neu' THEN 'Prospecting'
        WHEN opportunity_phase = 'qualifizierung' THEN 'Qualification'
        WHEN opportunity_phase = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN opportunity_phase = 'wertangebot' THEN 'Value Proposition'
        WHEN opportunity_phase = 'entscheidungsträger identifiziert' THEN 'Id. Decision Makers'
        WHEN opportunity_phase = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opportunity_phase = 'angebot/preiskalkulation' THEN 'Proposal/Price Quote'
        WHEN opportunity_phase = 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN opportunity_phase = 'gewonnen' THEN 'Closed Won'
        WHEN opportunity_phase = 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown phases as it is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(opportunity_close_date_raw, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(opportunity_close_date_raw, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(opportunity_close_date_raw, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '1900-01-01' -- Default date if unparseable, as "CloseDate" is NOT NULL
    ) AS "CloseDate",
    opportunity_amount AS "Amount",
    opportunity_currency AS "CurrencyIsoCode",
    customer_number AS "AccountId",
    opportunity_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cleaned_opportunities
