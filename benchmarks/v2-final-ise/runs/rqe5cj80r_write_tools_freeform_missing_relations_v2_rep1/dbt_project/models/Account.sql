{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE
        WHEN tier IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN tier
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }}
