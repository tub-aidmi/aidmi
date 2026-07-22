{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(tier)) IN ('gold', 'g', 'platinum member') THEN 'Gold'
        WHEN LOWER(TRIM(tier)) IN ('silver', 's', 'silver member') THEN 'Silver'
        WHEN LOWER(TRIM(tier)) IN ('bronze', 'b', 'bronze member') THEN 'Bronze'
        WHEN LOWER(TRIM(tier)) IN ('platinum', 'p') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    CAST(id AS TEXT) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}