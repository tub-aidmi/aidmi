{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Opportunity ' || o.id) AS "Name",
    CASE
        WHEN TRIM(o.stage) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(o.stage) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(o.stage) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(o.stage) = 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(o.stage) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(o.stage) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(o.stage) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(o.stage) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(o.stage) = 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(o.stage) = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown or NULL stages to satisfy NOT NULL
    END AS "StageName",
    '2000-01-01' AS "CloseDate", -- Placeholder, as no source data and target is NOT NULL
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default, as no source data
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    '2000-01-01' AS "CreatedDate", -- Placeholder, as no source data
    '2000-01-01' AS "LastModifiedDate", -- Placeholder, as no source data
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id
