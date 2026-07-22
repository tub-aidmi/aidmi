{{ config(materialized='table') }}

SELECT 
    SUBSTRING(MD5(proj_id), 1, 18) AS "Id",
    TRIM(name) AS "Name",
    CASE status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live 
        ELSE NULL 
    END AS "Go_Live_Date__c",
    SUBSTRING(MD5('ACC_' || proj.kd), 1, 18) AS "Account__c",
    SUBSTRING(MD5('OPP_' || chancen.chance_id), 1, 18) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} 
    ON proj.opp = chancen.chance_id
WHERE TRIM(name) IS NOT NULL AND TRIM(name) != ''
