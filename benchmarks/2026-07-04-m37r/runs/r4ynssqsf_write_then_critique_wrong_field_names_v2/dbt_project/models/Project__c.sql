{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    COALESCE(TRIM(p.name), 'Project ' || p.proj_id) AS "Name",
    CASE
        WHEN p.status = 'Active' THEN 'Active'
        WHEN p.status = 'Completed' THEN 'Completed'
        WHEN p.status = 'In Planning' THEN 'In Planning'
        WHEN p.status = 'On Hold' THEN 'On Hold'
        WHEN p.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
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