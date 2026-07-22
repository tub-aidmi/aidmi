{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    "Name" AS "Name",
    "Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN "Warranty_End_Date__c" IS NULL THEN NULL
        WHEN "Warranty_End_Date__c" = 'N/A' THEN NULL
        WHEN "Warranty_End_Date__c" = '0000-00-00' THEN NULL
        WHEN "Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_DATE("Warranty_End_Date__c", 'YYYYMMDD')::TEXT
        WHEN "Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN "Warranty_End_Date__c"
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE("Warranty_End_Date__c", 'DD.MM.YYYY')::TEXT
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("Warranty_End_Date__c", 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    "Account__c" AS "Account__c",
    "Project__c" AS "Project__c",
    NULL AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}