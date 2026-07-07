{{ config(materialized='table') }}

SELECT
    TRIM(proj.id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE UPPER(TRIM(proj.status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(proj.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(proj.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(proj.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    TRIM(proj.client_id) AS "Account__c",
    TRIM(proj.opportunity_ref) AS "Opportunity__c",
    TRIM(proj.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
