-- depends_on: {{ source('fixture_missing_relations_v2_src', 'project') }}

{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unknown Project Name') AS "Name",
    CASE
        WHEN UPPER(TRIM(project.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(project.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    project.client_id AS "Account__c",
    project.opportunity_ref AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project