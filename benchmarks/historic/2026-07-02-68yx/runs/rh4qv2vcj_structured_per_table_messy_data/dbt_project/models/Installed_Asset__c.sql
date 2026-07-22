{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    COALESCE(TRIM("Name"), 'Unknown Asset') AS "Name",
    CAST("Serial_Number__c" AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN TRIM("Warranty_End_Date__c") IS NULL THEN NULL
        WHEN TRIM("Warranty_End_Date__c") = 'N/A' THEN NULL
        WHEN TRIM("Warranty_End_Date__c") = '0000-00-00' THEN NULL
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM("Warranty_End_Date__c")
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM("Warranty_End_Date__c"), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM("Warranty_End_Date__c"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("Warranty_End_Date__c") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM("Warranty_End_Date__c"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST("Account__c" AS TEXT) AS "Account__c",
    CAST("Project__c" AS TEXT) AS "Project__c",
    NULL::TEXT AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}