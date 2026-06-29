
-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    COALESCE(src."Name", 'Unknown Asset') AS "Name",
    src."Serial_Number__c" AS "Serial_Number__c",
    CASE
        WHEN src."Warranty_End_Date__c" IS NULL THEN NULL
        WHEN src."Warranty_End_Date__c" IN ('N/A', '0000-00-00') THEN NULL
        WHEN src."Warranty_End_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src."Warranty_End_Date__c", 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src."Warranty_End_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src."Warranty_End_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src."Warranty_End_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src."Warranty_End_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src."Warranty_End_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."Warranty_End_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    src."Account__c" AS "Account__c",
    src."Project__c" AS "Project__c",
    NULL AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Installed_Asset__c') }} src
