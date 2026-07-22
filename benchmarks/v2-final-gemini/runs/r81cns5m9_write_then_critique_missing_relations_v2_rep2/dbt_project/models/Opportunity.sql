-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, o.id) AS "Name",
    CASE
        WHEN LOWER(o.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(o.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(o.stage) = 'qualified' THEN 'Qualification'
        WHEN LOWER(o.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(o.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(o.stage) = 'won' THEN 'Closed Won'
        WHEN LOWER(o.stage) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(o.stage) = 'lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a."Id" AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ ref('Account') }} AS a
ON
    o.customer_number = a."Legacy_Customer_ID__c"