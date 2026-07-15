{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    CASE 
        WHEN INITCAP(TRIM(COALESCE(name, ''))) = '' THEN 'Unnamed Project'
        ELSE INITCAP(TRIM(COALESCE(name, '')))
    END AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(go_live_date__c) = '' OR TRIM(go_live_date__c) IN ('N/A', '0000-00-00') THEN NULL
        WHEN LENGTH(TRIM(go_live_date__c)) = 8 AND go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}