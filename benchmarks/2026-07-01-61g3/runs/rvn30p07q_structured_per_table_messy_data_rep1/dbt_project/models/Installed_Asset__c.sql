{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    CASE
        WHEN TRIM(COALESCE("Name", '')) = '' THEN 'Unknown Asset'
        ELSE INITCAP(TRIM("Name"))
    END AS "Name",
    "Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN "Warranty_End_Date__c" IS NULL OR TRIM("Warranty_End_Date__c") = '' THEN NULL
        WHEN UPPER(TRIM("Warranty_End_Date__c")) = 'N/A' THEN NULL
        WHEN "Warranty_End_Date__c" = '0000-00-00' THEN NULL
         -- YYYYMMDD format (8 digits, starts with 2 or 1)
        WHEN "Warranty_End_Date__c" ~ '^\d{8}$'
            AND LENGTH("Warranty_End_Date__c") = 8 THEN
            TO_DATE("Warranty_End_Date__c", 'YYYYMMDD')::TEXT
         -- DD.MM.YYYY format
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_DATE("Warranty_End_Date__c", 'DD.MM.YYYY')::TEXT
         -- MM/DD/YYYY format (contains / with 4-digit year at end)
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE("Warranty_End_Date__c", 'MM/DD/YYYY')::TEXT
         -- ISO YYYY-MM-DD format (already correct, validate it's a real date)
        WHEN "Warranty_End_Date__c" ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN
            TO_DATE("Warranty_End_Date__c", 'YYYY-MM-DD')::TEXT
         -- Fallback: try common formats
        ELSE NULL
    END AS "Warranty_End_Date__c",
    "Account__c" AS "Account__c",
    "Project__c" AS "Project__c",
    "Id" AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}