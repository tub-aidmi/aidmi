-- This model transforms source opportunity data into the target Opportunity schema.
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for StageName if source is unexpected or NULL
    END AS "StageName",
    COALESCE(p.go_live, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate", -- project.go_live is assumed to be the close date
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- Not available in source
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted" -- Default to 0 as no source column
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON o.id = p.opportunity_ref
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON LOWER(TRIM(o.account_name)) = LOWER(TRIM(a.name))