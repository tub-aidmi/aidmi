-- noinspection SqlDialectInspection
-- noinspection SqlNoDataSourceInspection

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default to Prospecting for unmapped stages
    END AS "StageName",
    COALESCE(
        (CASE WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live ELSE NULL END),
        CAST(CURRENT_DATE AS TEXT) -- Non-sentinel default for NOT NULL CloseDate
    ) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId", -- Join to account table for Salesforce-style Id
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No source, default to NULL as allowed for text column
    NULL AS "LastModifiedDate", -- No source, default to NULL as allowed for text column
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON o.id = p.opportunity_ref
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON REPLACE(o.customer_number, 'KD-', 'ACC-') = a.id