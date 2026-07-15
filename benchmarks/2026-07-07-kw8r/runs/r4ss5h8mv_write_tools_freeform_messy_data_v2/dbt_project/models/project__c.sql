{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
     -- Map project_status__c to target enum
    CASE LOWER(TRIM(COALESCE(project_status__c, '')))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
     -- Parse go_live_date from multiple formats
    CASE
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN UPPER(TRIM(go_live_date__c)) IN ('N/A', '') THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{8}$' THEN
            SUBSTR(go_live_date__c, 1, 4) || '-' || SUBSTR(go_live_date__c, 5, 2) || '-' || SUBSTR(go_live_date__c, 7, 2)
        WHEN go_live_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
            LPAD(SPLIT_PART(go_live_date__c, '/', 3), 4, '0') || '-' ||
            LPAD(SPLIT_PART(go_live_date__c, '/', 1), 2, '0') || '-' ||
            LPAD(SPLIT_PART(go_live_date__c, '/', 2), 2, '0')
        WHEN go_live_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN
            SUBSTR(go_live_date__c, 7, 4) || '-' ||
            LPAD(SPLIT_PART(go_live_date__c, '.', 2), 2, '0') || '-' ||
            LPAD(SPLIT_PART(go_live_date__c, '.', 1), 2, '0')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM "fixture_messy_data_v2_src"."project__c"
