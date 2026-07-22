{{ config(materialized='table') }}

SELECT
  CAST(a."id" AS TEXT) AS "Id",
  a."name" AS "Name",
  NULL AS "ERP_Number__c",
  CASE
    WHEN LOWER(TRIM(a."tier")) = 'gold' THEN 'Gold'
    WHEN LOWER(TRIM(a."tier")) = 'silver' THEN 'Silver'
    WHEN LOWER(TRIM(a."tier")) = 'bronze' THEN 'Bronze'
    WHEN LOWER(TRIM(a."tier")) = 'platinum' THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  a."region" AS "Region__c",
  a."industry" AS "Industry",
  NULL AS "Website",
  NULL AS "BillingCity",
  NULL AS "BillingCountry",
  CAST(a."id" AS TEXT) AS "Legacy_Customer_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
