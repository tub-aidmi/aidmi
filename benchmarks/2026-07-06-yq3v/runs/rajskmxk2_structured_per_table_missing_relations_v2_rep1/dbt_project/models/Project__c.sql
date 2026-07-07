-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(status) = 'active' THEN 'Active'
        WHEN LOWER(status) = 'completed' THEN 'Completed'
        WHEN LOWER(status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN go_live
        WHEN go_live ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }}