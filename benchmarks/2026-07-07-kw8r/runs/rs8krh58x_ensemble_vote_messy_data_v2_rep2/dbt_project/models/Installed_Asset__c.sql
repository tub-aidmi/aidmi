{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    serial_number__c AS "Serial_Number__c",
    CASE 
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(warranty_end_date__c)
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN 
            SUBSTR(TRIM(warranty_end_date__c), 1, 4) || '-' || 
            SUBSTR(TRIM(warranty_end_date__c), 5, 2) || '-' || 
            SUBSTR(TRIM(warranty_end_date__c), 7, 2)
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}