{{ config(materialized='table') }}

SELECT
  id AS "Id",
  CASE 
    WHEN TRIM(name) IS NULL OR TRIM(name) = '' THEN 'Unknown'
    ELSE TRIM(INITCAP(name))
  END AS "Name",
  TRIM(erp_number__c) AS "ERP_Number__c",
  CASE 
    WHEN TRIM(LOWER(customer_tier__c)) = 'gold' THEN 'Gold'
    WHEN TRIM(LOWER(customer_tier__c)) = 'silver' THEN 'Silver'
    WHEN TRIM(LOWER(customer_tier__c)) = 'bronze' THEN 'Bronze'
    WHEN TRIM(LOWER(customer_tier__c)) = 'platinum' THEN 'Platinum'
    ELSE NULL 
  END AS "Customer_Tier__c",
  TRIM(INITCAP(region__c)) AS "Region__c",
  TRIM(INITCAP(industry)) AS "Industry",
  TRIM(website) AS "Website",
  TRIM(INITCAP(billingcity)) AS "BillingCity",
  TRIM(INITCAP(billingcountry)) AS "BillingCountry",
  id AS "Legacy_Customer_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}