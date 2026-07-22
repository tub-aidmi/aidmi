
{{ config(materialized='table') }}

SELECT
    T2."Id" AS "Id",
    COALESCE(T2."Name", 'Unnamed Asset') AS "Name",
    T2."Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN T2."Warranty_End_Date__c" IS NULL THEN NULL
        WHEN T2."Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN T2."Warranty_End_Date__c" -- Already in YYYY-MM-DD format
        WHEN T2."Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(T2."Warranty_End_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN T2."Warranty_End_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(T2."Warranty_End_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN T2."Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(T2."Warranty_End_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    T2."Account__c" AS "Account__c",
    T2."Project__c" AS "Project__c",
    CAST(NULL AS TEXT) AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }} AS T2
