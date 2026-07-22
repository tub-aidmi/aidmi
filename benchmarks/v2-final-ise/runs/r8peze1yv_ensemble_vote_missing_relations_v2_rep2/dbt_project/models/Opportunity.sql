{{ config(materialized='table') }}

SELECT 
    opp.id AS "Id",
    COALESCE(NULLIF(TRIM(opp.name), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(opp.stage)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.stage)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(opp.stage)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.stage)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.stage)) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.stage)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.stage)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.stage)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(opp.stage)) IN ('CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.stage)) IN ('CLOSED LOST') THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    '2025-12-31' AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.customer_number AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} opp
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(opp.account_name) = TRIM(acc.name)
    OR TRIM(opp.customer_number) = TRIM(acc.id)