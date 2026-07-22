{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unknown Project') AS "Name",
    CASE TRIM(LOWER(project.project_status__c))
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
    CASE
        WHEN project.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN project.go_live_date__c
        WHEN project.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    project.account__c AS "Account__c",
    project.opportunity__c AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project
