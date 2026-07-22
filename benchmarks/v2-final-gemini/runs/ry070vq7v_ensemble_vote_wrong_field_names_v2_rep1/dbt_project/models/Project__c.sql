{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    proj.name AS "Name",
    CASE
        WHEN LOWER(proj.status) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(proj.status) IN ('completed', 'abgeschlossen', 'fertig') THEN 'Completed'
        WHEN LOWER(proj.status) IN ('in planning', 'geplant') THEN 'In Planning'
        WHEN LOWER(proj.status) IN ('on hold', 'angehalten', 'pausiert') THEN 'On Hold'
        WHEN LOWER(proj.status) IN ('cancelled', 'storniert', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    -- Attempt to parse date in multiple formats, default to NULL if unparseable
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live -- YYYY-MM-DD
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    proj.kd AS "Account__c", -- This will be mapped to a Salesforce Account Id in a later transformation step
    proj.opp AS "Opportunity__c", -- This will be mapped to a Salesforce Opportunity Id in a later transformation step
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj