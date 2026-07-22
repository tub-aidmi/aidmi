{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown Project') AS "Name",
    CASE
        WHEN INITCAP(LOWER(TRIM(project_status__c))) = 'Active' THEN 'Active'
        WHEN INITCAP(LOWER(TRIM(project_status__c))) = 'Completed' THEN 'Completed'
        WHEN INITCAP(LOWER(TRIM(project_status__c))) = 'In Planning' THEN 'In Planning'
        WHEN INITCAP(LOWER(TRIM(project_status__c))) = 'On Hold' THEN 'On Hold'
        WHEN INITCAP(LOWER(TRIM(project_status__c))) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN CAST(NULL AS TEXT)
        -- Try DD.MM.YYYY format
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        -- Try YYYY-MM-DD or YYYY/MM/DD format
        WHEN go_live_date__c ~ '^\d{4}[\-\/]\d{1,2}[\-\/]\d{1,2}$'
            THEN TO_DATE(
                REGEXP_REPLACE(TRIM(go_live_date__c), '[\-\/]', '-', 'g'),
                'YYYY-MM-DD'
            )::TEXT
        -- Try MM/DD/YYYY format
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        -- Try YYYYMMDD format
        WHEN go_live_date__c ~ '^\d{8}$'
            THEN TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD')::TEXT
        ELSE CAST(NULL AS TEXT)
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}