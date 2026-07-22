{{
  config(
    materialized='table'
  )
}}

SELECT
  MD5(master_kunden.kundennummer) AS "Id",
  COALESCE(master_kunden.unternehmensname, 'Unknown Account') AS "Name",
  master_kunden.erp_nr AS "ERP_Number__c",
  CASE
    WHEN LOWER(master_kunden.kundenklasse) = 'gold' THEN 'Gold'
    WHEN LOWER(master_kunden.kundenklasse) = 'silver' THEN 'Silver'
    WHEN LOWER(master_kunden.kundenklasse) = 'bronze' THEN 'Bronze'
    WHEN LOWER(master_kunden.kundenklasse) = 'platinum' THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  master_kunden.vertriebsgebiet AS "Region__c",
  master_kunden.industrie AS "Industry",
  master_kunden.homepage AS "Website",
  master_kunden.stadt AS "BillingCity",
  master_kunden.land_region AS "BillingCountry",
  master_kunden.kundennummer AS "Legacy_Customer_ID__c",
  NOW()::TEXT AS "CreatedDate",
  NOW()::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_kunden') }} AS master_kunden
