{{
  config(
    materialized='table'
  )
}}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN stage = 'Prospecting' THEN 'Prospecting'
        WHEN stage = 'Qualification' THEN 'Qualification'
        WHEN stage = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN stage = 'Value Proposition' THEN 'Value Proposition'
        WHEN stage = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN stage = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN stage = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN stage = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN stage = 'Closed Won' THEN 'Closed Won'
        WHEN stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate", -- No direct source, provide a default for NOT NULL
    amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- No direct source, provide a default
    customer_number AS "AccountId", -- Assuming customer_number maps to Account.Id
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
