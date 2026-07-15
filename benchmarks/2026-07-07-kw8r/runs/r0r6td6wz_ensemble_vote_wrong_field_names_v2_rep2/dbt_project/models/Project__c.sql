{{ config(materialized='table') }}
SELECT 
    MD5(proj.proj_id) AS "Id",
    TRIM(proj.name) AS "Name",
    CASE 
        WHEN LOWER(TRIM(proj.status)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(proj.status)) IN ('completed', 'abgeschlossen', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(proj.status)) IN ('in planning', 'geplant', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(proj.status)) IN ('on hold', 'pausiert', 'angehalten') THEN 'On Hold'
        WHEN LOWER(TRIM(proj.status)) IN ('cancelled', 'storniert', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN proj.go_live IS NOT NULL AND TRIM(proj.go_live) != '' THEN 
            COALESCE(
                CASE WHEN TRIM(proj.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(proj.go_live) END,
                CASE WHEN TRIM(proj.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(proj.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD') END,
                CASE WHEN TRIM(proj.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(proj.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD') END,
                CASE WHEN TRIM(proj.go_live) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(REGEXP_REPLACE(TRIM(proj.go_live), '(\d{4})(\d{2})(\d{2})', '\1-\2-\3'), 'YYYY-MM-DD'), 'YYYY-MM-DD') END,
                NULL
            )
        ELSE NULL 
    END AS "Go_Live_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    MD5(chancen.chance_id) AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden ON proj.kd = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen ON proj.opp = chancen.chance_id