{{ config(materialized='table') }}

SELECT
    "Id",
    COALESCE("Name", 'Unknown') AS "Name",
    CASE LOWER(TRIM("Project_Status__c"))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'inactive' THEN 'On Hold'
        WHEN 'inaktiv' THEN 'On Hold'
        WHEN 'pending' THEN 'In Planning'
        WHEN 'in bearbeitung' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN "Go_Live_Date__c" IS NULL OR "Go_Live_Date__c" = '' OR "Go_Live_Date__c" = 'N/A' OR "Go_Live_Date__c" = '0000-00-00' THEN NULL
        WHEN "Go_Live_Date__c" ~ '^\d{4}-\d{2}-\d{2}$' AND CAST(SUBSTRING("Go_Live_Date__c", 1, 4) AS INTEGER) > 0 THEN TO_DATE("Go_Live_Date__c", 'YYYY-MM-DD')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("Go_Live_Date__c", 'MM/DD/YYYY')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE("Go_Live_Date__c", 'DD.MM.YYYY')::TEXT
        WHEN "Go_Live_Date__c" ~ '^\d{8}$' AND CAST(SUBSTRING("Go_Live_Date__c", 1, 4) AS INTEGER) > 0 THEN TO_DATE("Go_Live_Date__c", 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    "Account__c",
    "Opportunity__c",
    CASE WHEN "Id" ~ '^PROJ-(\d+)$' THEN SUBSTRING("Id", 6) ELSE NULL END AS "Legacy_Project_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Project__c') }}