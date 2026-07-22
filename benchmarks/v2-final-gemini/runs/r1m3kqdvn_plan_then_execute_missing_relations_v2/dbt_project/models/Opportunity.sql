{{ config(materialized='table') }}

SELECT
    TRIM(o.id) AS "Id",
    COALESCE(TRIM(o.name), 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- Default for NOT NULL with no source
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- No direct source
    TRIM(a.id) AS "AccountId", -- Mapped from account.id via join
    TRIM(o.id) AS "Legacy_Opportunity_ID__c", -- Source natural key
    NULL AS "CreatedDate", -- No direct source
    NULL AS "LastModifiedDate", -- No direct source
    0 AS "IsDeleted" -- Default to FALSE
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(o.customer_number) = TRIM(a.id)
