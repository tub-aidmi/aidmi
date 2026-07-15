{{ config(materialized='table') }}

SELECT
  UPPER(TRIM(REGEXP_REPLACE(id, '^[^A-Z0-9]+', '', 'i'))) AS "Id",
  INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
  NULLIF(TRIM(erp_number__c), '') AS "ERP_Number__c",
  CASE 
    WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'silver', 'bronze', 'platinum') 
    THEN INITCAP(TRIM(customer_tier__c)) 
    ELSE 'Bronze' 
  END AS "Customer_Tier__c",
  INITCAP(TRIM(region__c)) AS "Region__c",
  TRIM(industry) AS "Industry",
  LOWER(TRIM(website)) AS "Website",
  INITCAP(TRIM(billingcity)) AS "BillingCity",
  UPPER(TRIM(billingcountry)) AS "BillingCountry",
  id AS "Legacy_Customer_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}