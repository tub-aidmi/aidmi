{{ config(materialized='table') }}

SELECT
    MD5(p.proj_id) AS "Id",
    COALESCE(p.name, 'No Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kunden_nr) AS "Account__c",
    MD5(c.chance_id) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k ON p.kd = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c ON p.opp = c.chance_id
