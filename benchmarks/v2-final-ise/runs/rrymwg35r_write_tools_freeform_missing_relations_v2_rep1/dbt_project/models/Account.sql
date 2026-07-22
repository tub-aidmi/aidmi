{{ config(materialized='table') }}

SELECT
    CAST(a."id" AS TEXT) AS "Id",
    COALESCE(a."name", '') AS "Name",
    NULL AS "ERP_Number__c",
    CASE LOWER(TRIM(a."tier"))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(a."region")) AS "Region__c",
    UPPER(INITCAP(TRIM(a."industry"))) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    CAST(a."id" AS TEXT) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
