{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unknown') AS "Name",
    CASE 
        WHEN o.stage IS NULL OR TRIM(o.stage) = '' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) LIKE '%closed won%' OR LOWER(TRIM(o.stage)) = 'won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) LIKE '%closed lost%' OR LOWER(TRIM(o.stage)) IN ('lost', 'cancelled') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stage)) LIKE '%negotiat%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) LIKE '%proposal%' OR LOWER(TRIM(o.stage)) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) LIKE '%perception%' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) LIKE '%decision maker%' OR LOWER(TRIM(o.stage)) IN ('idm', 'ident. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) LIKE '%value prop%' OR LOWER(TRIM(o.stage)) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) LIKE '%need%' OR LOWER(TRIM(o.stage)) = 'analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) LIKE '%qualif%' THEN 'Qualification'
        ELSE 'Prospecting'
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'USD'::TEXT AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(LOWER(o.account_name)) = TRIM(LOWER(a.name))