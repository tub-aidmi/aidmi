{{ config(materialized='table') }}

SELECT 
    src_asset.id AS "Id",
    COALESCE(NULLIF(TRIM(src_asset.name), ''), 'Unknown Asset') AS "Name",
    TRIM(src_asset.serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN src_asset.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN src_asset.warranty_end_date__c
        WHEN src_asset.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src_asset.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src_asset.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src_asset.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src_asset.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src_asset.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    src_asset.account__c AS "Account__c",
    src_asset.project__c AS "Project__c",
    TRIM(src_asset.serial_number__c) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS src_asset