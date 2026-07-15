{{ config(materialized='table') }}

SELECT
    CAST(proj_id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown Project') AS "Name",
    status AS "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$'
            THEN go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}