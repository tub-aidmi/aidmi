
{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    COALESCE(src."Name", 'Unnamed Project') AS "Name",
    CASE
        WHEN TRIM(UPPER(src."Project_Status__c")) = 'ACTIVE' THEN 'Active'
        WHEN TRIM(UPPER(src."Project_Status__c")) = 'COMPLETED' THEN 'Completed'
        WHEN TRIM(UPPER(src."Project_Status__c")) = 'IN PLANNING' THEN 'In Planning'
        WHEN TRIM(UPPER(src."Project_Status__c")) = 'ON HOLD' THEN 'On Hold'
        WHEN TRIM(UPPER(src."Project_Status__c")) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src."Go_Live_Date__c" = '0000-00-00' THEN NULL
        WHEN src."Go_Live_Date__c" IS NOT NULL AND src."Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN src."Go_Live_Date__c"
        WHEN src."Go_Live_Date__c" IS NOT NULL AND src."Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src."Go_Live_Date__c" IS NOT NULL AND src."Go_Live_Date__c" ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'MM-DD-YYYY'), 'YYYY-MM-DD')
        WHEN src."Go_Live_Date__c" IS NOT NULL AND src."Go_Live_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src."Go_Live_Date__c" IS NOT NULL AND src."Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src."Account__c" AS "Account__c",
    src."Opportunity__c" AS "Opportunity__c",
    CAST(NULL AS TEXT) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Project__c') }} AS src
