-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    source."Id" AS "Id",
    COALESCE(source."Name", 'Unknown') AS "Name",
    source."Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN source."Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN source."Warranty_End_Date__c"
        WHEN source."Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(source."Warranty_End_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN source."Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(source."Warranty_End_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    source."Account__c" AS "Account__c",
    source."Project__c" AS "Project__c",
    source."Id" AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Installed_Asset__c') }} AS source