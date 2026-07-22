{{ config(materialized='table') }}
SELECT
    proj.proj_id AS "Id",
    TRIM(proj.name) AS "Name",
    CASE
        WHEN TRIM(LOWER(proj.status)) IN ('aktiv') THEN 'Active'
        WHEN TRIM(LOWER(proj.status)) IN ('abgeschlossen') THEN 'Completed'
        WHEN TRIM(LOWER(proj.status)) IN ('in planung') THEN 'In Planning'
        WHEN TRIM(LOWER(proj.status)) IN ('in wartestellung') THEN 'On Hold'
        WHEN TRIM(LOWER(proj.status)) IN ('storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden.kunden_nr AS "Account__c",
    chancen.chance_id AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kunden
    ON TRIM(proj.kd) = TRIM(kunden.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} chancen
    ON TRIM(proj.opp) = TRIM(chancen.chance_id)