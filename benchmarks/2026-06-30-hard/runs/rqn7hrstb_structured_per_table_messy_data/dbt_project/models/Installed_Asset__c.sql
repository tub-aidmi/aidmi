
{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown Asset Name') AS "Name",
    "Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN "Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE("Warranty_End_Date__c", 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN "Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("Warranty_End_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("Warranty_End_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "Warranty_End_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("Warranty_End_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    "Account__c" AS "Account__c",
    "Project__c" AS "Project__c",
    NULL::TEXT AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    NULL::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}