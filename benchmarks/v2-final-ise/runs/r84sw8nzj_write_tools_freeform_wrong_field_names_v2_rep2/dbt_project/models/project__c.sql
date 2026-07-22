{{ config(materialized='table') }}

SELECT
    'a1X' || LPAD(REGEXP_REPLACE(proj_id, '\D', '', 'g'), 12, '0') AS "Id",
    INITCAP(name) AS "Name",
    CASE status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    '001' || LPAD(REGEXP_REPLACE(kd, '\D', '', 'g'), 12, '0') AS "Account__c",
    '006' || LPAD(REGEXP_REPLACE(opp, '\D', '', 'g'), 12, '0') AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
