{{ config(materialized='table') }}

WITH source_opportunity AS (
    SELECT
        id,
        name,
        stage,
        amount,
        customer_number,
        account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),
source_account AS (
    SELECT
        id,
        name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(o.stage) LIKE '%qualif%' THEN 'Qualification'
        WHEN LOWER(o.stage) LIKE '%needs%' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) LIKE '%value%' THEN 'Value Proposition'
        WHEN LOWER(o.stage) LIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) LIKE '%perception%' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) LIKE '%negotiation%' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(o.stage) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for NOT NULL
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- Defaulting to current date as no source column is available and it's NOT NULL
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    sa.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_opportunity o
LEFT JOIN
    source_account sa ON o.customer_number = sa.id
