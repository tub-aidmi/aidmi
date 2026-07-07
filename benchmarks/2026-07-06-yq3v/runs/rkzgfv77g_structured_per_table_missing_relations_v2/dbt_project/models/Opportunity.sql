{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN o.stage = 'New' THEN 'Prospecting'
        WHEN o.stage = 'Qualified' THEN 'Qualification'
        WHEN o.stage = 'Analysis' THEN 'Needs Analysis'
        WHEN o.stage = 'Value Prop' THEN 'Value Proposition'
        WHEN o.stage = 'Decision Makers' THEN 'Id. Decision Makers'
        WHEN o.stage = 'Perception' THEN 'Perception Analysis'
        WHEN o.stage = 'Proposal' THEN 'Proposal/Price Quote'
        WHEN o.stage = 'Negotiation' THEN 'Negotiation/Review'
        WHEN o.stage = 'Won' THEN 'Closed Won'
        WHEN o.stage = 'Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    -- CloseDate is NOT NULL but no direct source. Using a default date string.
    '1900-01-01' AS "CloseDate",
    o.amount AS "Amount",
    -- CurrencyIsoCode has no direct source. Defaulting to 'USD'.
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.customer_number = a.id
