{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    TRIM(COALESCE(name, 'Unknown')) AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'active'      THEN 'Active'
        WHEN 'completed'   THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold'     THEN 'On Hold'
        WHEN 'cancelled'   THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    go_live_date__c AS "Go_Live_Date__c",
    account__c      AS "Account__c",
    opportunity__c  AS "Opportunity__c",
    id              AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0               AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}