-- This model transforms project data from the source system into the Project__c target schema.
{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    COALESCE(TRIM(p.name), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(p.status) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(p.status) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(p.status) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(p.status) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(p.status) IN ('cancelled', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live, 'YYYY-MM-DD')
            WHEN p.go_live ~ '^\d{2}.\d{2}.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')
            WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
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