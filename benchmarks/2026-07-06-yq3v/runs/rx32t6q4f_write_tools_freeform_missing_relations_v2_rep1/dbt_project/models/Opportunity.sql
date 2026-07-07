{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(o.stage) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(o.stage) = 'qualification' THEN 'Qualification'
            WHEN LOWER(o.stage) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(o.stage) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(o.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN LOWER(o.stage) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(o.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN LOWER(o.stage) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN LOWER(o.stage) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(o.stage) = 'closed lost' THEN 'Closed Lost'
            ELSE 'Prospecting' -- Default for NOT NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(NULL, '2000-01-01') AS "CloseDate", -- No source date, use default
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId", -- Join to account table to get Salesforce-style Account Id
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id -- Assuming customer_number maps to account.id
