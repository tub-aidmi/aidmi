{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        -- Skip obvious invalid dates
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' OR LOWER(TRIM(go_live_date__c)) = 'n/a' OR go_live_date__c = '0000-00-00' THEN NULL
        -- YYYY-MM-DD format (ISO, 10 chars)
        WHEN LENGTH(TRIM(go_live_date__c)) = 10 AND TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        -- YYYYMMDD format (8 digits)
        WHEN LENGTH(TRIM(go_live_date__c)) = 8 AND TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        -- MM/DD/YYYY format (US, variable length but contains '/')
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- DD.MM.YYYY format (European, contains '.')
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}