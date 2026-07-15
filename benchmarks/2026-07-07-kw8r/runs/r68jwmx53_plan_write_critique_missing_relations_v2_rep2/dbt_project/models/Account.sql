{{ config(materialized='table') }}
SELECT
    id AS "Id",
    INITCAP(name) AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN UPPER(tier) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM')
        THEN INITCAP(LOWER(tier))
        ELSE NULL
    END AS "Customer_Tier__c",
    UPPER(region) AS "Region__c",
    INITCAP(industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}