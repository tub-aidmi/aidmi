-- depends_on: {{ source('fixture_messy_data_v2_src', 'account') }}
{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    TRIM(industry) AS "Industry",
    TRIM(website) AS "Website",
    TRIM(billingcity) AS "BillingCity",
    TRIM(billingcountry) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}