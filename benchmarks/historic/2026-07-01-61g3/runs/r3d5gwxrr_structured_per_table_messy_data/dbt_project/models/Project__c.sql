{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(NULLIF(TRIM("Name"), ''), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM("Project_Status__c")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('in bearbeitung', 'pending') THEN 'In Planning'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('inactive', 'inaktiv', 'on hold', 'paused') THEN 'On Hold'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('cancelled', 'abgebrochen', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN "Go_Live_Date__c" IS NULL OR TRIM("Go_Live_Date__c") = '' THEN NULL
        WHEN TRIM("Go_Live_Date__c") IN ('N/A', 'n/a', 'NA', 'na', '-', '--', '0000-00-00') THEN NULL
        -- YYYYMMDD format (8 digits)
        WHEN "Go_Live_Date__c" ~ '^\d{8}$' THEN TO_DATE(TRIM("Go_Live_Date__c"), 'YYYYMMDD')::TEXT
        -- MM/DD/YYYY or M/D/YYYY format
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY')::TEXT
        -- DD.MM.YYYY or D.M.YYYY format
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY')::TEXT
        -- ISO YYYY-MM-DD format (already valid)
        WHEN "Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM("Go_Live_Date__c"), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    "Account__c" AS "Account__c",
    "Opportunity__c" AS "Opportunity__c",
    NULL::TEXT AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Project__c') }}