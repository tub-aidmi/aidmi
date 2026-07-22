{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Gold' THEN 'Gold'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Silver' THEN 'Silver'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Bronze' THEN 'Bronze'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    TRIM(industry) AS "Industry",
    LOWER(TRIM(website)) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}
