
{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), '') AS "Name",
    CASE
        WHEN UPPER(TRIM("Project_Status__c")) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM("Project_Status__c")) IN ('INACTIVE', 'INAKTIV') THEN 'Cancelled'
        WHEN UPPER(TRIM("Project_Status__c")) IN ('IN BEARBEITUNG', 'PENDING') THEN 'In Planning'
        WHEN UPPER(TRIM("Project_Status__c")) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM("Project_Status__c")) = 'ON HOLD' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN "Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN "Go_Live_Date__c" -- YYYY-MM-DD
        WHEN "Go_Live_Date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("Go_Live_Date__c", 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("Go_Live_Date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("Go_Live_Date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD') -- D.M.YYYY or DD.MM.YYYY
        ELSE NULL
    END AS "Go_Live_Date__c",
    "Account__c" AS "Account__c",
    "Opportunity__c" AS "Opportunity__c",
    CAST(NULL AS TEXT) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Project__c') }}
