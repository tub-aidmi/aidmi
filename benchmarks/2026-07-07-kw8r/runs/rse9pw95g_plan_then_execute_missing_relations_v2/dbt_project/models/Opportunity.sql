{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(INITCAP(o.name)), 'Unknown Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.stage)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stage)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(o.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stage)) IN ('VALUE PROP', 'VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stage)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stage)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stage)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(o.customer_number) = TRIM(a.id)