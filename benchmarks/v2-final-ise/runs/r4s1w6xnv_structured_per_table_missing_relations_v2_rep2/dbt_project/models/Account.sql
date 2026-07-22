{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Unknown') AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(tier)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM')
        THEN INITCAP(UPPER(TRIM(tier)))
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE WHEN TRIM(region) = '' OR region IS NULL THEN NULL ELSE TRIM(region) END AS "Region__c",
    CASE WHEN TRIM(industry) = '' OR industry IS NULL THEN NULL ELSE TRIM(industry) END AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}