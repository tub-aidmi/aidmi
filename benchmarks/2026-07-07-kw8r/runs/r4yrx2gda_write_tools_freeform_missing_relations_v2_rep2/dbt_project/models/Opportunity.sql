{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stage)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stage)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(o.stage)) IN ('NEEDS ANALYSIS', 'NEEDS_ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stage)) IN ('VALUE PROPOSITION', 'VALUE_PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stage)) IN ('ID. DECISION MAKERS', 'ID_DECISION_MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stage)) IN ('PERCEPTION ANALYSIS', 'PERCEPTION_ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stage)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stage)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION', 'REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stage)) IN ('CLOSED WON', 'CLOSED_WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stage)) IN ('CLOSED LOST', 'CLOSED_LOST') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    COALESCE(
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.id = o.customer_number LIMIT 1),
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.name = o.account_name LIMIT 1)
    ) AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o