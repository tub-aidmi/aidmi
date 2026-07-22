{{ config(materialized='table') }}

SELECT
    REGEXP_REPLACE(id, '^[A-Z]+-', '') AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    INITCAP(TRIM(tier)) AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}