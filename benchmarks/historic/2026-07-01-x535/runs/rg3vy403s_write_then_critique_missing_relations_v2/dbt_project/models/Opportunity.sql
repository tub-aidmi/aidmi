{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN o.stage ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN o.stage ILIKE 'Qualification' THEN 'Qualification'
        WHEN o.stage ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN o.stage ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CAST('2000-01-01' AS TEXT) AS "CloseDate", -- Default date for NOT NULL column
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- Not in source, nullable
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not in source, nullable
    NULL AS "LastModifiedDate", -- Not in source, nullable
    0 AS "IsDeleted" -- Default for NOT NULL integer
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id
