{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identifying decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for NOT NULL
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId", -- Corrected: Join to account to get Salesforce-style Account.Id
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id
