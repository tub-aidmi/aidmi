{{ config(materialized='table') }}

SELECT
    ENCODE(SHA256(chancen.chance_id::BYTEA), 'hex') AS "Id",
    TRIM(chancen.bezeichnung) AS "Name",
    CASE
        WHEN UPPER(TRIM(chancen.phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(chancen.phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(chancen.phase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(chancen.phase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(chancen.phase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(chancen.phase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(chancen.phase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(chancen.phase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(chancen.phase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(chancen.phase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    ENCODE(SHA256(kunden.kunden_nr::BYTEA), 'hex') AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
ON
    chancen.kd_nr = kunden.kunden_nr
