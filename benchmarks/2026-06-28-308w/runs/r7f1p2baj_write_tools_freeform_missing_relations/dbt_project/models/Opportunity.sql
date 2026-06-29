{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Untitled Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(TRIM(stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stage)) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(stage)) = 'closed won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    '1970-01-01' AS "CloseDate", -- CloseDate is NOT NULL, no source
    amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default
    COALESCE(customer_number, account_name) AS "AccountId", -- Prioritize customer_number if available
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Opportunity') }}
