{{ config(materialized='table') }}

SELECT 
    SUBSTRING(MD5(asset_id), 1, 18) AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE WHEN garantie_bis IS NOT NULL AND garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' 
         THEN garantie_bis 
         ELSE NULL 
    END AS "Warranty_End_Date__c",
    SUBSTRING(MD5('ACC_' || k.kunden_nr), 1, 18) AS "Account__c",
    SUBSTRING(MD5('PRJ_' || p.proj_id), 1, 18) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
     '2024-01-01' AS "CreatedDate",
     '2024-01-01' AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
    ON a.projekt_ref = p.proj_id
