-- depends_on: {{ source('fixture_messy_data_src', 'Account') }}

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), 'Unknown') AS "Name",
    "ERP_Number__c" AS "ERP_Number__c",
    CASE UPPER(TRIM(COALESCE("Customer_Tier__c", '')))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    "Region__c" AS "Region__c",
    "Industry" AS "Industry",
    "Website" AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }}