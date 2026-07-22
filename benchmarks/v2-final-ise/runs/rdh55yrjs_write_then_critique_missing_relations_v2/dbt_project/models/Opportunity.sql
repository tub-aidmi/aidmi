{{ config(materialized='table') }}
SELECT 
    opp.id AS "Id",
    COALESCE(NULLIF(TRIM(opp.name), ''), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN TRIM(LOWER(opp.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(opp.stage)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(opp.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(opp.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(opp.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(opp.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(opp.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(opp.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(opp.stage)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(opp.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        NULLIF(TRIM(opp.close_date), ''),
        TO_CHAR(CURRENT_DATE + INTERVAL '30 days', 'YYYY-MM-DD')
    ) AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.customer_number AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc 
    ON TRIM(opp.account_name) = TRIM(acc.name)