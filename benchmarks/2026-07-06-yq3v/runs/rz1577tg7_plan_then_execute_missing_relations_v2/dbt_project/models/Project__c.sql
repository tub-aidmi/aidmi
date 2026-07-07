SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(project.status) = 'active' THEN 'Active'
        WHEN LOWER(project.status) = 'completed' THEN 'Completed'
        WHEN LOWER(project.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(project.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(project.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(project.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(project.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    project.client_id AS "Account__c",
    project.opportunity_ref AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project