{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'N/A') AS "Name",
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown stages, since it's NOT NULL
    END AS "StageName",
    CAST('1900-01-01' AS TEXT) AS "CloseDate", -- No direct source, using a fixed default for NOT NULL
    o.amount AS "Amount",
    CAST('USD' AS TEXT) AS "CurrencyIsoCode", -- No direct source, using 'USD' as a default
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CAST('1900-01-01' AS TEXT) AS "CreatedDate", -- No direct source, using a fixed default for NOT NULL
    CAST('1900-01-01' AS TEXT) AS "LastModifiedDate", -- No direct source, using a fixed default for NOT NULL
    0 AS "IsDeleted" -- Default to 0 for boolean flag
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.account_name = a.name
