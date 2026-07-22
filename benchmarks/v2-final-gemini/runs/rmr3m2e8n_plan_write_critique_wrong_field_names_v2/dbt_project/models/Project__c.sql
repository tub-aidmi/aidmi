{{ config(materialized='table') }}

SELECT
    TRIM(proj.proj_id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE TRIM(UPPER(proj.status))
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN_PLANUNG' THEN 'In Planning'
        WHEN 'IN_HALT' THEN 'On Hold'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(proj.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    TRIM(proj.kd) AS "Account__c",
    TRIM(proj.opp) AS "Opportunity__c",
    TRIM(proj.proj_id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
