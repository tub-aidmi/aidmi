-- dbt model for Opportunity
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN TRIM(LOWER(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stage)) IN ('id. decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stage)) IN ('proposal/price quote', 'proposal price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stage)) IN ('negotiation/review', 'negotiation review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default stage as it is a NOT NULL field
    END AS "StageName",
    CAST(CURRENT_DATE AS TEXT) AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    CAST('USD' AS TEXT) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id