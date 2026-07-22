{{ config(materialized='table') }}

SELECT
    src_project.id AS "Id",
    COALESCE(TRIM(src_project.name), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(src_project.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(src_project.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(src_project.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(src_project.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(src_project.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src_project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src_project.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src_project.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src_project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src_project.go_live ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(src_project.go_live, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        WHEN src_project.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src_project.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    target_account."Id" AS "Account__c",
    target_opportunity."Id" AS "Opportunity__c",
    src_project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS src_project
LEFT JOIN
    {{ ref('Account') }} AS target_account
    ON src_project.client_id = target_account."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Opportunity') }} AS target_opportunity
    ON src_project.opportunity_ref = target_opportunity."Legacy_Opportunity_ID__c"