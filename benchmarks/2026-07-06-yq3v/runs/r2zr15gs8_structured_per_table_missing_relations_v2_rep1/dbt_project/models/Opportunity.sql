-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolve

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    COALESCE(o.stage, 'Prospecting') AS "StageName",
    COALESCE(p.go_live, CURRENT_DATE::text) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(LOWER(o.account_name)) = TRIM(LOWER(a.name))
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON o.id = p.opportunity_ref