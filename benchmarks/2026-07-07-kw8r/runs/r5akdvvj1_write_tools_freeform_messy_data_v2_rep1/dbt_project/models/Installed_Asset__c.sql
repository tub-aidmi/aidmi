{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(a.serial_number__c), '') AS "Serial_Number__c",
    CASE 
        WHEN a.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty_end_date__c
        WHEN a.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    a.account__c AS "Account__c",
    a.project__c AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} a