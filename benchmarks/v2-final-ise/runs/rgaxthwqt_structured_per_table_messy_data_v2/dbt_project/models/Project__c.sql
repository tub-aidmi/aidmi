{{ config(materialized='table') }}

SELECT
    CAST(p.id AS TEXT) AS "Id",
    INITCAP(TRIM(p.name)) AS "Name",
    CASE LOWER(TRIM(p.project_status__c))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'planning' THEN 'In Planning'
        WHEN 'on_hold' THEN 'On Hold'
        WHEN 'on-hold' THEN 'On Hold'
        WHEN 'cancel' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_date__c IS NOT NULL AND TRIM(p.go_live_date__c) != ''
             AND p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(p.go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN p.go_live_date__c IS NOT NULL AND TRIM(p.go_live_date__c) != ''
             AND p.go_live_date__c ~ '^\d{8}$'
            THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYYMMDD')::TEXT
        WHEN p.go_live_date__c IS NOT NULL AND TRIM(p.go_live_date__c) != ''
             AND p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(TRIM(p.go_live_date__c), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(p.account__c AS TEXT) AS "Account__c",
    CAST(p.opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(p.id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p