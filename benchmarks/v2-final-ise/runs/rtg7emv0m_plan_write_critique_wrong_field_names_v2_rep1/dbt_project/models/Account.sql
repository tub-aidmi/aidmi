{{ config(materialized='table') }}

SELECT
  TRIM(UPPER("kunden_nr")) AS "Id",
  COALESCE(TRIM(INITCAP("firmenname")), 'Unknown Customer') AS "Name",
  TRIM("erp_nummer") AS "ERP_Number__c",
  CASE 
    WHEN TRIM(UPPER("kategorie")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(TRIM("kategorie"))
    ELSE NULL 
  END AS "Customer_Tier__c",
  TRIM("gebiet") AS "Region__c",
  TRIM(INITCAP("branche")) AS "Industry",
  TRIM("webseite") AS "Website",
  TRIM("ort") AS "BillingCity",
  TRIM("land") AS "BillingCountry",
  TRIM("kunden_nr") AS "Legacy_Customer_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}