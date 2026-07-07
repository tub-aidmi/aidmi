-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(TRIM(proj.name), 'Untitled Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(proj.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(proj.status)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(proj.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(proj.status)) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(TRIM(proj.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(proj.status)) = 'in planung' THEN 'In Planning'
        WHEN LOWER(TRIM(proj.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(proj.status)) = 'pausiert' THEN 'On Hold'
        WHEN LOWER(TRIM(proj.status)) = 'cancelled' THEN 'Cancelled'
        WHEN LOWER(TRIM(proj.status)) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live IS NULL THEN NULL
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(proj.go_live::DATE, 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
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