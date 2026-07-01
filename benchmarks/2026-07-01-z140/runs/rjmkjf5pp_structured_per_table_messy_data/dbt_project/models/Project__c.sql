{{ config(materialized='table') }}

SELECT
    "Id",
    COALESCE(TRIM("Name"), 'Unnamed Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM("Project_Status__c")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('inactive', 'inaktiv') THEN 'Cancelled'
        WHEN LOWER(TRIM("Project_Status__c")) = 'in bearbeitung' THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) = 'pending' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN "Go_Live_Date__c" IS NULL OR TRIM("Go_Live_Date__c") = '' THEN NULL
        WHEN TRIM("Go_Live_Date__c") = 'N/A' THEN NULL
        WHEN TRIM("Go_Live_Date__c") ~ '^0000-00-00$' THEN NULL
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{4}-\d{2}-\d{2}$' 
            AND TO_DATE(TRIM("Go_Live_Date__c"), 'YYYY-MM-DD') IS NOT NULL 
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
            AND TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY') IS NOT NULL 
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{2}\.\d{2}\.\d{4}$' 
            AND TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY') IS NOT NULL 
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("Go_Live_Date__c") ~ '^\d{8}$' 
            AND TO_DATE(TRIM("Go_Live_Date__c"), 'YYYYMMDD') IS NOT NULL 
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    "Account__c" AS "Account__c",
    "Opportunity__c" AS "Opportunity__c",
    "Id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Project__c') }}