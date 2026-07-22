{{ config(materialized='table') }}

SELECT
  '00I' || REGEXP_REPLACE(TRIM(p.id), '[^0-9]', '', 'g') AS "Id",
  COALESCE(TRIM(p.name), 'Unnamed Project') AS "Name",
  CASE
    WHEN p.project_status__c IS NULL THEN NULL
    ELSE INITCAP(
      CASE LOWER(TRIM(p.project_status__c))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
      END
    )
  END AS "Project_Status__c",
  CASE
    WHEN p.go_live_date__c IS NULL OR TRIM(p.go_live_date__c) = '' THEN NULL
    WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live_date__c, 'YYYY-MM-DD')::TEXT
    WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live_date__c, 'DD.MM.YYYY')::TEXT
    WHEN p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live_date__c, 'MM/DD/YYYY')::TEXT
    WHEN p.go_live_date__c ~ '^\d{8}$' THEN 
      CASE 
        WHEN LENGTH(TRIM(p.go_live_date__c)) = 8 
        THEN TO_DATE(p.go_live_date__c, 'YYYYMMDD')::TEXT
        ELSE NULL
      END
    ELSE NULL
  END AS "Go_Live_Date__c",
  (SELECT '001' || REGEXP_REPLACE(TRIM(ac2.id), '[^0-9]', '', 'g') 
   FROM {{ source('fixture_messy_data_v2_src', 'account') }} ac2
   WHERE REGEXP_REPLACE(TRIM(ac2.erp_number__c), '[^0-9]', '', 'g') = REGEXP_REPLACE(TRIM(p.account__c), '[^0-9]', '', 'g')
   LIMIT 1) AS "Account__c",
  CASE WHEN TRIM(COALESCE(p.opportunity__c, '')) != '' 
    THEN '006' || REGEXP_REPLACE(TRIM(p.opportunity__c), '[^0-9]', '', 'g')
    ELSE NULL
  END AS "Opportunity__c",
  CAST(p.id AS TEXT) AS "Legacy_Project_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p