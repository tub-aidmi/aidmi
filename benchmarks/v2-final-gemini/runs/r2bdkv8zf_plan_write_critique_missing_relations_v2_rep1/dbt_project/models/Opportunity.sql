{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(TRIM(opp.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL constraint
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- Default for NOT NULL, formatted to ISO 8601
    opp.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- Not in source
    acc."Id" AS "AccountId", -- Joined via customer_number to get the target Account ID
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not in source
    NULL AS "LastModifiedDate", -- Not in source
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN {{ ref('Account') }} AS acc
    ON SUBSTRING(opp.customer_number FROM 4) = SUBSTRING(acc."Legacy_Customer_ID__c" FROM 5)