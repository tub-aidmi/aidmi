{{ config(materialized='table') }}

SELECT
  CAST(id AS TEXT) AS "Id",
  CASE WHEN TRIM(name) = '' OR name IS NULL THEN 'Unknown' ELSE INITCAP(TRIM(name)) END AS "Name",
  CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
  CASE LOWER(TRIM(customer_tier__c))
    WHEN 'gold' THEN 'Gold'
    WHEN 'silver' THEN 'Silver'
    WHEN 'bronze' THEN 'Bronze'
    WHEN 'platinum' THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  CAST(LOWER(TRIM(region__c)) AS TEXT) AS "Region__c",
  CAST(INITCAP(TRIM(industry)) AS TEXT) AS "Industry",
  CAST(TRIM(website) AS TEXT) AS "Website",
  CAST(INITCAP(TRIM(billingcity)) AS TEXT) AS "BillingCity",
  CAST(INITCAP(TRIM(billingcountry)) AS TEXT) AS "BillingCountry",
  CAST(legacy_customer_id__c AS TEXT) AS "Legacy_Customer_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}