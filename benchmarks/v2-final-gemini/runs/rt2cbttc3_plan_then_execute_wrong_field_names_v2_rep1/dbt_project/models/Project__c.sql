-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(proj.status) = 'active' THEN 'Active'
        WHEN LOWER(proj.status) = 'completed' THEN 'Completed'
        WHEN LOWER(proj.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(proj.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(proj.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(proj.kd) AS "Account__c",
    MD5(proj.opp) AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj