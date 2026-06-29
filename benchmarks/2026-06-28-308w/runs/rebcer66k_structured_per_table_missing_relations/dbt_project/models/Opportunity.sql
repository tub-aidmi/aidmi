
{{ config(materialized='table') }}

SELECT
    src_opportunity.id AS "Id",
    COALESCE(src_opportunity.name, '') AS "Name",
    COALESCE(src_opportunity.stage, 'Prospecting') AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    src_opportunity.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    src_account.id AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Opportunity') }} AS src_opportunity
LEFT JOIN
    {{ source('fixture_missing_relations_src', 'Account') }} AS src_account
    ON src_opportunity.account_name = src_account.name
