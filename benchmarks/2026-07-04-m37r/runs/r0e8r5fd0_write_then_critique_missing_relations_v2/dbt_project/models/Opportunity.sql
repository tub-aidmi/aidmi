-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    COALESCE(o.stage, 'Prospecting') AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON SUBSTRING(o.customer_number FROM POSITION('-' IN o.customer_number) + 1) = SUBSTRING(a.id FROM POSITION('-' IN a.id) + 1)