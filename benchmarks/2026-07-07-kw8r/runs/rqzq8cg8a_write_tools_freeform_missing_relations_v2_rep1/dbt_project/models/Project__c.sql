{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE LOWER(TRIM(status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
        WHEN go_live ~ '^\d{8}$' THEN SUBSTRING(go_live, 1, 4) || '-' || SUBSTRING(go_live, 5, 2) || '-' || SUBSTRING(go_live, 7, 2)
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
