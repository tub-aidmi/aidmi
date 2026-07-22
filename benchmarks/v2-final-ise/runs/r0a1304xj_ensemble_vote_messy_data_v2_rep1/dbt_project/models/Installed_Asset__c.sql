{{ config(materialized='table') }}

SELECT 
    src.id AS "Id",
    src.name AS "Name",
    src.serial_number__c AS "Serial_Number__c",
    CASE 
        WHEN src.warranty_end_date__c IS NULL THEN NULL
        WHEN src.warranty_end_date__c = '0000-00-00' THEN NULL
        WHEN src.warranty_end_date__c = 'N/A' THEN NULL
        WHEN src.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            TO_CHAR(TO_DATE(src.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(src.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(src.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(src.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    src.account__c AS "Account__c",
    src.project__c AS "Project__c",
    src.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS src