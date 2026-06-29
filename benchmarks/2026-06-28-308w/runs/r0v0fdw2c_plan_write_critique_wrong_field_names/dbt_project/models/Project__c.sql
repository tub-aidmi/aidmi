
{{ config(materialized='table') }}

SELECT
    TRIM(p.proj_id) AS "Id",
    COALESCE(TRIM(p.name), 'Unnamed Project') AS "Name",
    CASE TRIM(LOWER(p.status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CAST(p.go_live AS DATE), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    TRIM(k.kunden_nr) AS "Account__c",
    TRIM(c.chance_id) AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'chancen') }} AS c
    ON TRIM(p.opp) = TRIM(c.chance_id)