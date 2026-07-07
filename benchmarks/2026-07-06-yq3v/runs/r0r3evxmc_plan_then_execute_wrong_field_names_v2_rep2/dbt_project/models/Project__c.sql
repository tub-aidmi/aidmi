-- {{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    COALESCE(proj.name, 'Unknown Project') AS "Name",
    CASE LOWER(proj.status)
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(TO_DATE(proj.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    MD5(chancen.chance_id) AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON proj.kd = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON proj.opp = chancen.chance_id