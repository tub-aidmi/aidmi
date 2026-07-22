{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN o.stage IS NOT NULL THEN INITCAP(o.stage)
        ELSE 'Prospecting'
    END AS "StageName",
    '1970-01-01' AS "CloseDate",
    o.amount AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(LOWER(o.account_name)) = TRIM(LOWER(a.name))
