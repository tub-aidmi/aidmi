{{ config(materialized='table') }}

SELECT
    ENCODE(SHA256(proj.proj_id::bytea), 'hex') AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE LOWER(proj.status)
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(proj.go_live, 'YYYY-MM-DD')
            WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(proj.go_live, 'DD.MM.YYYY')
            WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(proj.go_live, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
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