{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",

    CASE
        WHEN "Name" IS NULL THEN 'Unknown Project'
        ELSE REGEXP_REPLACE(TRIM("Name"), ' Impl\.\s*$', '', 'g')
    END AS "Name",

    CASE
        WHEN LOWER(TRIM("Project_Status__c")) = 'active' THEN 'Active'
        WHEN LOWER(TRIM("Project_Status__c")) = 'in bearbeitung' THEN 'In Planning'
        WHEN LOWER(TRIM("Project_Status__c")) IN ('inactive', 'inaktiv') THEN 'On Hold'
        WHEN LOWER(TRIM("Project_Status__c")) = 'pending' THEN 'In Planning'
        WHEN LOWER(TRIM("Project_Status__c")) = 'aktiv' THEN 'Active'
        ELSE NULL
    END AS "Project_Status__c",

    CASE
        WHEN "Go_Live_Date__c" IS NULL THEN NULL
        WHEN TRIM("Go_Live_Date__c") = 'N/A' THEN NULL
        WHEN TRIM("Go_Live_Date__c") = '0000-00-00' THEN NULL

        -- ISO YYYY-MM-DD (skip sentinel 0000 dates)
        WHEN "Go_Live_Date__c" ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'
            AND SUBSTRING("Go_Live_Date__c" FROM 1 FOR 4)::INTEGER > 0
            THEN "Go_Live_Date__c"

        -- MM/DD/YYYY or M/D/YYYY (e.g. "10/25/2026", "1/26/2025")
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY'), 'YYYY-MM-DD')

        -- DD.MM.YYYY European format (e.g. "26.10.2026")
        WHEN "Go_Live_Date__c" ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY'), 'YYYY-MM-DD')

        -- YYYYMMDD compact format (e.g. "20270217")
        WHEN "Go_Live_Date__c" ~ '^\d{8}$'
            AND SUBSTRING("Go_Live_Date__c" FROM 1 FOR 4)::INTEGER BETWEEN 1 AND 9999
            AND SUBSTRING("Go_Live_Date__c" FROM 5 FOR 2)::INTEGER BETWEEN 1 AND 12
            AND SUBSTRING("Go_Live_Date__c" FROM 7 FOR 2)::INTEGER BETWEEN 1 AND 31
            THEN SUBSTRING("Go_Live_Date__c" FROM 1 FOR 4) || '-' ||
                 SUBSTRING("Go_Live_Date__c" FROM 5 FOR 2) || '-' ||
                 SUBSTRING("Go_Live_Date__c" FROM 7 FOR 2)

        ELSE NULL
    END AS "Go_Live_Date__c",

    CAST("Account__c" AS TEXT) AS "Account__c",
    CAST("Opportunity__c" AS TEXT) AS "Opportunity__c",
    CAST("Id" AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Project__c') }}