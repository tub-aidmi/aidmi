{{ config(materialized='table') }}

SELECT
    "Id",
    COALESCE(TRIM("Name"), 'Unknown Account') AS "Name",
    "ERP_Number__c",
    CASE INITCAP(TRIM(COALESCE("Customer_Tier__c", '')))
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silver' THEN 'Silver'
        WHEN 'Platinum' THEN 'Platinum'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE WHEN TRIM(COALESCE("Region__c", '')) = '' THEN NULL ELSE "Region__c" END AS "Region__c",
    "Industry",
    "Website",
    "BillingCity",
    "BillingCountry",
    "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Account') }}