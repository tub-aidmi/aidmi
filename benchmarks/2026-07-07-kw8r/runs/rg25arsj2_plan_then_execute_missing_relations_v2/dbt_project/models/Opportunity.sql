{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Untitled Opportunity') AS "Name",
    CASE LOWER(TRIM(stage))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL::TEXT AS "CloseDate",
    COALESCE(
        CASE
            WHEN amount IS NOT NULL AND amount > 0 THEN amount
            ELSE NULL
        END,
        NULL
    ) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    'ACC-' || REGEXP_REPLACE(TRIM(customer_number), '[^0-9]', '', 'g') AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}