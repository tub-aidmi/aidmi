{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unnamed Account') AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    tier AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}