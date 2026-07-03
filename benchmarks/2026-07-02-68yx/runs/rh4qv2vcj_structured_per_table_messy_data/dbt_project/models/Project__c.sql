{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",

    CASE
        WHEN TRIM("Name") IS NULL OR TRIM("Name") = '' THEN 'Unknown Project'
        ELSE REGEXP_REPLACE(TRIM("Name"), '\s+Impl\.\s*$', '', 'g')
    END AS "Name",

    CASE LOWER(TRIM("Project_Status__c"))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'in bearbeitung' THEN 'In Planning'
        WHEN 'pending' THEN 'In Planning'
        WHEN 'inactive' THEN 'On Hold'
        WHEN 'inaktiv' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",

    CASE
        WHEN TRIM("Go_Live_Date__c") IS NULL OR TRIM("Go_Live_Date__c") = '' THEN NULL
        WHEN TRIM("Go_Live_Date__c") = 'N/A' THEN NULL
        WHEN TRIM("Go_Live_Date__c") = '0000-00-00' THEN NULL

        -- ISO YYYY-MM-DD (skip sentinel 0000 dates)
        WHEN "Go_Live_Date__c" ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'
            AND SUBSTRING(TRIM("Go_Live_Date__c") FROM 1 FOR 4)::INTEGER > 0
            THEN TRIM("Go_Live_Date__c")

        -- MM/DD/YYYY or M/D/YYYY (e.g. "1/16/2028", "10/25/2026")
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'MM/DD/YYYY'), 'YYYY-MM-DD')

        -- DD.MM.YYYY European format (e.g. "02.02.2025", "26.10.2026")
        WHEN "Go_Live_Date__c" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("Go_Live_Date__c"), 'DD.MM.YYYY'), 'YYYY-MM-DD')

        -- YYYYMMDD compact format (e.g. "20270217")
        WHEN "Go_Live_Date__c" ~ '^\d{8}$'
            THEN SUBSTRING(TRIM("Go_Live_Date__c") FROM 1 FOR 4) || '-' ||
                 SUBSTRING(TRIM("Go_Live_Date__c") FROM 5 FOR 2) || '-' ||
                 SUBSTRING(TRIM("Go_Live_Date__c") FROM 7 FOR 2)

        ELSE NULL
    END AS "Go_Live_Date__c",

    CAST("Account__c" AS TEXT) AS "Account__c",
    CAST("Opportunity__c" AS TEXT) AS "Opportunity__c",
    CAST("Id" AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Project__c') }}