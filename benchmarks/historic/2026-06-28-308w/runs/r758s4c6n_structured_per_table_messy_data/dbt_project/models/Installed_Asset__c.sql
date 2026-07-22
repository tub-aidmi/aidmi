
{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), 'Unknown Asset') AS "Name",
    TRIM("Serial_Number__c") AS "Serial_Number__c",
    CASE
        WHEN "Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE("Warranty_End_Date__c", 'YYYY-MM-DD')::TEXT
        WHEN "Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_DATE("Warranty_End_Date__c", 'YYYYMMDD')::TEXT
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("Warranty_End_Date__c", 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM("Account__c") AS "Account__c",
    TRIM("Project__c") AS "Project__c",
    "Id" AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}
