{{ config(materialized='table') }}

SELECT 
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis IS NOT NULL AND TRIM(garantie_bis) <> ''
            THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::DATE::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    kd_ref AS "Account__c",
    projekt_ref AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
WHERE asset_id IS NOT NULL 
  AND TRIM(asset_id) <> ''