{{ config(materialized='table') }}

SELECT
    TRIM(o.id) AS "Id",
    COALESCE(TRIM(o.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default to 'Prospecting' to satisfy NOT NULL constraint
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- No source, default to current date
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- No direct source
    TRIM(a.id) AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No direct source
    NULL AS "LastModifiedDate", -- No direct source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    TRIM(REPLACE(o.customer_number, 'KD-', 'ACC-')) = TRIM(a.id)
