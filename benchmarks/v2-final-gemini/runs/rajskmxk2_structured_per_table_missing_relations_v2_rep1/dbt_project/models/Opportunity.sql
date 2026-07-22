-- depends_on: {{ source('fixture_missing_relations_v2_src', 'account') }}

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), o.id) AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        ELSE 'Prospecting' -- Default stage for NOT NULL target
    END AS "StageName",
    '1900-01-01' AS "CloseDate", -- No source column, using a placeholder date as it's NOT NULL
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default value as no source column is available
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
        ON TRIM(o.customer_number) = TRIM(a.id)