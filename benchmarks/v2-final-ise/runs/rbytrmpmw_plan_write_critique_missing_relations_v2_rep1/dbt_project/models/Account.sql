{{ config(materialized='table') }}

SELECT
    CAST(TRIM("id") AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM("name")), 'Unknown') AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    CASE LOWER(TRIM("tier"))
        WHEN 'gold' THEN 'Gold'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'elite' THEN 'Platinum'
        WHEN 'silver' THEN 'Silver'
        WHEN 'standard' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'basic' THEN 'Bronze'
        WHEN 'entry' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("region")) AS "Region__c",
    INITCAP(TRIM("industry")) AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    CAST(TRIM("id") AS TEXT) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}