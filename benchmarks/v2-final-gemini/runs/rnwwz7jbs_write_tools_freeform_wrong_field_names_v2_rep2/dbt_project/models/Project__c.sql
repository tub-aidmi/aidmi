{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name",
    CASE
        WHEN status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN status
        ELSE NULL
    END AS "Project_Status__c",
    -- Assuming go_live is already in YYYY-MM-DD format based on inspection
    go_live AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
