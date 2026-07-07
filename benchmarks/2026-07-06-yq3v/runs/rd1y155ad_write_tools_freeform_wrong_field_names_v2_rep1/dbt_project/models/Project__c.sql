{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    COALESCE(p.name, 'N/A') AS "Name",
    CASE
        WHEN LOWER(p.status) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(p.status) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(p.status) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(p.status) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(p.status) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE 'In Planning' -- Default to a valid status
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(
            TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        NULL -- Prefer NULL for dates when no default is specified and it's nullable
    ) AS "Go_Live_Date__c",
    p.kd AS "Account__c", -- Account__c is kunden_nr from the kunden table
    p.opp AS "Opportunity__c", -- Opportunity__c is chance_id from the chancen table
    p.proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
WHERE
    p.proj_id IS NOT NULL
    AND COALESCE(p.name, '') != ''
