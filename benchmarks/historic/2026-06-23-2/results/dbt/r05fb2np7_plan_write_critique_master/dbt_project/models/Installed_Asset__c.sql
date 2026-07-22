{{ config(materialized='table') }}

SELECT 
    asset_kennung AS Id,
    TRIM(asset_name) AS Name,
    serien_nummer AS Serial_Number__c,
    CASE 
        WHEN garantieende IS NULL OR garantieende IN ('0000-00-00', '0000', '') THEN NULL 
        ELSE garantieende 
    END AS Warranty_End_Date__c,
    kunden_kennung AS Account__c,
    projekt_kennung AS Project__c,
    asset_kennung AS Legacy_Asset_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0::integer AS IsDeleted
FROM {{ source('fixture_master_src', 'master_assets') }}