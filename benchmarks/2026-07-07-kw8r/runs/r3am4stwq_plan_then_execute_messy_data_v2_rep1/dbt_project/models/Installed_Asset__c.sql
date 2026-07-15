{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    TRIM(name) AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN warranty_end_date__c IS NULL OR warranty_end_date__c = 'N/A' OR warranty_end_date__c = '0000-00-00' THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty_end_date__c
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN warranty_end_date__c ~ '^[0-9]+\.[0-9]+\.[0-9]+$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN warranty_end_date__c ~ '^[0-9]+/[0-9]+/[0-9]+$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(project__c) AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}