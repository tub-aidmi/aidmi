{{ config(materialized='table') }}

SELECT
    CAST("id" AS TEXT) AS "Id",
    INITCAP(TRIM("name")) AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    CASE LOWER(TRIM(COALESCE("tier", '')))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("region")) AS "Region__c",
    INITCAP(TRIM("industry")) AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    CAST("id" AS TEXT) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}