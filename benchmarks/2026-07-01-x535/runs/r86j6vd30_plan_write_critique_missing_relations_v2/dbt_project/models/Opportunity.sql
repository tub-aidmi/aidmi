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
    COALESCE(o.name, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN o.stage IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN o.stage
        ELSE 'Prospecting' -- Default if source stage is NULL or invalid
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- No source, using current date as default since it's NOT NULL
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- No source, defaulting to USD
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_opportunity AS o
LEFT JOIN source_account AS a
    ON o.account_name = a.name
