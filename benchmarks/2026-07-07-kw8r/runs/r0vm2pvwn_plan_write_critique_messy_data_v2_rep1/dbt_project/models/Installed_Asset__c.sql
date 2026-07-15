{{ config(materialized='table') }}

SELECT 
    TRIM(id) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown Asset') AS "Name",
    UPPER(TRIM(serial_number__c)) AS "Serial_Number__c",
    CASE 
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) IN ('', 'N/A') THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND warranty_end_date__c != '0000-00-00' THEN warranty_end_date__c
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        WHEN warranty_end_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(warranty_end_date__c, 'M/D/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(project__c) AS "Project__c",
    TRIM(id) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}