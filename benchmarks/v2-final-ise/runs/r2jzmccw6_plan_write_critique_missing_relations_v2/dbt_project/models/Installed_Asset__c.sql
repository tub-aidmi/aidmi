{{ config(materialized='table') }}
WITH asset_account_mapping AS (
  SELECT 
    a.id AS asset_id,
    acc.id AS account_id,
    ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY CASE WHEN a.client = acc.id THEN 1 ELSE 2 END) AS rn
  FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
  LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON a.client = acc.id OR a.client = acc.name
)
SELECT 
  a.id AS "Id",
  a.name AS "Name",
  a.serial AS "Serial_Number__c",
  CASE WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty ELSE NULL END AS "Warranty_End_Date__c",
  aam.account_id AS "Account__c",
  p.id AS "Project__c",
  a.id AS "Legacy_Asset_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN asset_account_mapping aam ON a.id = aam.asset_id AND aam.rn = 1
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p ON a.project = p.id