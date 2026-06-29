
{{ config(materialized='table') }}

SELECT
    src.Id AS "Id",
    COALESCE(TRIM(src.Name), src.Id) AS "Name",
    CASE
        WHEN UPPER(TRIM(src."Project_Status__c")) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(src."Project_Status__c")) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(src."Project_Status__c")) IN ('IN PLANNING', 'IN BEARBEITUNG', 'PENDING') THEN 'In Planning'
        WHEN UPPER(TRIM(src."Project_Status__c")) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(src."Project_Status__c")) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src."Go_Live_Date__c" = '0000-00-00' THEN NULL -- Handle invalid zero date
        WHEN src."Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src."Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src."Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD') -- New format for DD.MM.YYYY
        WHEN src."Go_Live_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src."Go_Live_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src."Account__c" AS "Account__c",
    src."Opportunity__c" AS "Opportunity__c",
    NULL AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Project__c') }} AS src
