-- depends_on: removed as per instructions to avoid circular dependencies and errors
{{ config(materialized='table') }}

SELECT
  id AS "Id",
  COALESCE(TRIM(name), 'Unknown') AS "Name",
  CAST(NULL AS TEXT) AS "ERP_Number__c",
  CASE
    WHEN LOWER(tier) = 'gold' THEN 'Gold'
    WHEN LOWER(tier) = 'silver' THEN 'Silver'
    WHEN LOWER(tier) = 'bronze' THEN 'Bronze'
    WHEN LOWER(tier) = 'platinum' THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  TRIM(region) AS "Region__c",
  TRIM(industry) AS "Industry",
  CAST(NULL AS TEXT) AS "Website",
  CAST(NULL AS TEXT) AS "BillingCity",
  CAST(NULL AS TEXT) AS "BillingCountry",
  id AS "Legacy_Customer_ID__c",
  CAST(NULL AS TEXT) AS "CreatedDate",
  CAST(NULL AS TEXT) AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}