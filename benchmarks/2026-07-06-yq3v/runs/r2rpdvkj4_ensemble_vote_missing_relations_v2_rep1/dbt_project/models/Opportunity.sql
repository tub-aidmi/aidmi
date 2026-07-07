-- This dbt model transforms raw opportunity data into the target Opportunity schema.
-- It handles data type conversions, provides default values for missing NOT NULL columns,
-- and maps source stage names to target enum values.

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) IN ('qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identifying decision makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal_price_quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation_review', 'negotiation', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'closed_won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'closed_lost', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value as StageName is NOT NULL
    END AS "StageName",
    '1900-01-01' AS "CloseDate", -- Default value as CloseDate is NOT NULL and no source column
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default value as no source column
    a.id AS "AccountId", -- Joined from account table to get the Salesforce-style Account ID
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No source column provided
    NULL AS "LastModifiedDate", -- No source column provided
    0 AS "IsDeleted" -- Default value for boolean/integer
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.customer_number = a.id