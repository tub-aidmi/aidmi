
{{ config(materialized='table') }}

SELECT
    s."Id" AS "Id",
    COALESCE(s."Name", 'Unknown Account Name') AS "Name",
    s."ERP_Number__c" AS "ERP_Number__c",
    CASE TRIM(LOWER(s."Customer_Tier__c"))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    s."Region__c" AS "Region__c",
    s."Industry" AS "Industry",
    s."Website" AS "Website",
    s."BillingCity" AS "BillingCity",
    s."BillingCountry" AS "BillingCountry",
    s."Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }} AS s
