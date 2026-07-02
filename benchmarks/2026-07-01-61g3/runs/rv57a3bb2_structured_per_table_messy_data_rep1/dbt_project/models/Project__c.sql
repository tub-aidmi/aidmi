{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM("Project_Status__c")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('in planning', 'pending', 'in bearbeitung', 'in progress') THEN 'In Planning'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('on hold', 'angehalten', 'pause', 'gestoppt') THEN 'On Hold'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('cancelled', 'storniert', 'inactive', 'inaktiv') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN "Go_Live_Date__c" IS NULL OR TRIM("Go_Live_Date__c") = '' THEN NULL
        WHEN LOWER(TRIM("Go_Live_Date__c")) IN ('n/a', 'na') THEN NULL
        WHEN "Go_Live_Date__c" ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN "Go_Live_Date__c" ~ '^\d{8}$' THEN TO_DATE("Go_Live_Date__c", 'YYYYMMDD')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE("Go_Live_Date__c", 'YYYY-MM-DD')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("Go_Live_Date__c", 'MM/DD/YYYY')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE("Go_Live_Date__c", 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    "Account__c" AS "Account__c",
    "Opportunity__c" AS "Opportunity__c",
    NULL AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Project__c') }}