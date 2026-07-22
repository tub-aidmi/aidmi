{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(stage) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(stage) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(stage) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(stage) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(stage) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(stage) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(stage) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(stage) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(stage) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(stage) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for NOT NULL StageName
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate", -- NOT NULL, defaulting to current date as no source date is provided.
    amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    customer_number AS "AccountId", -- Assuming customer_number maps to Account.Id
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
