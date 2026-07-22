
{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    COALESCE(src."Name", 'Unknown Account') AS "Name",
    src."ERP_Number__c" AS "ERP_Number__c",
    CASE LOWER(src."Customer_Tier__c")
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    src."Region__c" AS "Region__c",
    src."Industry" AS "Industry",
    src."Website" AS "Website",
    src."BillingCity" AS "BillingCity",
    src."BillingCountry" AS "BillingCountry",
    src."Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }} src
