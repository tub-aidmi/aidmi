{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), '') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'in_progress', 'ongoing') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'complete', 'finished', 'closed') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planning', 'planned', 'not started') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'paused', 'suspended', 'stopped') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'canceled', 'void') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NOT NULL AND TRIM(go_live_date__c) <> '' THEN
            COALESCE(
                CASE WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT END,
                CASE WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT END,
                CASE WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT END,
                CASE WHEN LENGTH(REGEXP_REPLACE(go_live_date__c, '[^0-9]', '', 'g')) = 8 AND REGEXP_REPLACE(go_live_date__c, '[^0-9]', '', 'g') ~ '^\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$' THEN TO_DATE(REGEXP_REPLACE(go_live_date__c, '[^0-9]', '', 'g'), 'YYYYMMDD')::TEXT END
            )
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}