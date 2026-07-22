
{{ config(materialized='table') }}

SELECT
    TRIM("Id") AS "Id",
    COALESCE(TRIM("Name"), 'Unknown Project Name') AS "Name",
    CASE
        WHEN LOWER(TRIM("Project_Status__c")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('inactive', 'inaktiv') THEN 'Cancelled'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('in bearbeitung', 'pending') THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM("Go_Live_Date__c") IS NULL OR TRIM("Go_Live_Date__c") IN ('N/A', '0000-00-00', '') THEN NULL
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM("Account__c") AS "Account__c",
    TRIM("Opportunity__c") AS "Opportunity__c",
    NULL AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Project__c') }}
