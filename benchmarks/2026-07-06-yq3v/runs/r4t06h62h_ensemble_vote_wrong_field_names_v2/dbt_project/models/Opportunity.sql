{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    COALESCE(chance.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(UPPER(chance.phase)) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN TRIM(UPPER(chance.phase)) = 'ANGEBOTSPHASE' THEN 'Proposal/Price Quote'
        WHEN TRIM(UPPER(chance.phase)) = 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN TRIM(UPPER(chance.phase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN TRIM(UPPER(chance.phase)) = 'VERLOREN' THEN 'Closed Lost'
        WHEN TRIM(UPPER(chance.phase)) = 'BEDÜRFNISANALYSE' THEN 'Needs Analysis'
        WHEN TRIM(UPPER(chance.phase)) = 'IDENTIFIZIERUNG VON ENTSCHEIDUNGSTRÄGERN' THEN 'Id. Decision Makers'
        WHEN TRIM(UPPER(chance.phase)) = 'WERTEVORSCHLAG' THEN 'Value Proposition'
        WHEN TRIM(UPPER(chance.phase)) = 'PERZEPTIONSANALYSE' THEN 'Perception Analysis'
        WHEN TRIM(UPPER(chance.phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN TRIM(UPPER(chance.phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN TRIM(UPPER(chance.phase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN TRIM(UPPER(chance.phase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN TRIM(UPPER(chance.phase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN TRIM(UPPER(chance.phase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN TRIM(UPPER(chance.phase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN TRIM(UPPER(chance.phase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN TRIM(UPPER(chance.phase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN TRIM(UPPER(chance.phase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN chance.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(chance.abschlussdatum AS DATE), 'YYYY-MM-DD')
        WHEN chance.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(chance.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN chance.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(chance.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    END AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance