{{ config(materialized='table') }}

SELECT
    UPPER(TRIM("id")) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM("name")), ''), 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(COALESCE("tier", ''))) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(COALESCE("tier", ''))) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(COALESCE("tier", ''))) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(COALESCE("tier", ''))) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(COALESCE("region", ''))) AS "Region__c",
    INITCAP(TRIM(COALESCE("industry", ''))) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    TRIM("id") AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}