{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM("name")), ''), 'Unknown') AS "Name",
    TRIM("erp_number__c") AS "ERP_Number__c",
    CASE
        WHEN TRIM("customer_tier__c") ~* 'gold' THEN 'Gold'
        WHEN TRIM("customer_tier__c") ~* 'silver' THEN 'Silver'
        WHEN TRIM("customer_tier__c") ~* 'bronze' THEN 'Bronze'
        WHEN TRIM("customer_tier__c") ~* 'plat' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("region__c") AS "Region__c",
    TRIM("industry") AS "Industry",
    TRIM("website") AS "Website",
    TRIM("billingcity") AS "BillingCity",
    TRIM("billingcountry") AS "BillingCountry",
    TRIM("legacy_customer_id__c") AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}