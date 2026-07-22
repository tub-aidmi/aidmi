{{ config(materialized='table') }}

SELECT
    "Id",
    INITCAP(TRIM(COALESCE("Name", 'Unknown'))) AS "Name",
    TRIM("Serial_Number__c") AS "Serial_Number__c",
    CASE
        WHEN "Warranty_End_Date__c" IS NULL OR TRIM("Warranty_End_Date__c") = '' THEN NULL
        WHEN TRIM("Warranty_End_Date__c") = 'N/A' THEN NULL
        WHEN TRIM("Warranty_End_Date__c") ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_DATE(TRIM("Warranty_End_Date__c"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_DATE(TRIM("Warranty_End_Date__c"), 'DD.MM.YYYY')::TEXT
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{8}$'
            THEN TO_DATE(TRIM("Warranty_End_Date__c"), 'YYYYMMDD')::TEXT
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{4}-\d{2}-\d{2}$'
            THEN CAST("Warranty_End_Date__c" AS DATE)::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM("Account__c") AS "Account__c",
    TRIM("Project__c") AS "Project__c",
    NULL::TEXT AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}