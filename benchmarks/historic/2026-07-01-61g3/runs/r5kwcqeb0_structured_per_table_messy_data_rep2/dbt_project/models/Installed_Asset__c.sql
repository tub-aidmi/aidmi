{{ config(materialized='table') }}

SELECT
    CAST(Id AS TEXT) AS "Id",
    TRIM(COALESCE(Name, 'Unnamed Asset')) AS "Name",
    TRIM(Serial_Number__c) AS "Serial_Number__c",
    CASE
        WHEN Warranty_End_Date__c IS NULL THEN NULL
        WHEN TRIM(Warranty_End_Date__c) = '' THEN NULL
        WHEN TRIM(LOWER(Warranty_End_Date__c)) = 'n/a' THEN NULL
        WHEN TRIM(Warranty_End_Date__c) = '0000-00-00' THEN NULL
        WHEN Warranty_End_Date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TO_DATE(Warranty_End_Date__c, 'YYYY-MM-DD') AS TEXT)
        WHEN Warranty_End_Date__c ~ '^\d{8}$' THEN CAST(TO_DATE(Warranty_End_Date__c, 'YYYYMMDD') AS TEXT)
        WHEN Warranty_End_Date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN CAST(TO_DATE(Warranty_End_Date__c, 'MM/DD/YYYY') AS TEXT)
        WHEN Warranty_End_Date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN CAST(TO_DATE(Warranty_End_Date__c, 'DD.MM.YYYY') AS TEXT)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(Account__c AS TEXT) AS "Account__c",
    CAST(Project__c AS TEXT) AS "Project__c",
    CAST(Id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Installed_Asset__c') }}