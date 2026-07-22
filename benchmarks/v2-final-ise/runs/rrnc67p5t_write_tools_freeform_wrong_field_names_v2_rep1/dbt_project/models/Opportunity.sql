{{ config(materialized='table') }}

SELECT
    CAST('006' || c.chance_id AS TEXT) AS "Id",
    c.bezeichnung AS "Name",
    CASE 
        WHEN UPPER(TRIM(c.phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    c.abschlussdatum AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    CAST('001' || k.kunden_nr AS TEXT) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON c.kd_nr = k.kunden_nr
