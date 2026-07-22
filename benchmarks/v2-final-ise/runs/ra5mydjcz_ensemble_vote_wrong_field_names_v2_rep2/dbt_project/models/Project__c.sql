{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    proj.name AS "Name",
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
            TO_CHAR(
                COALESCE(
                    TO_DATE(TRIM(proj.go_live), 'DD.MM.YYYY'),
                    TO_DATE(TRIM(proj.go_live), 'YYYY-MM-DD'),
                    TO_DATE(TRIM(proj.go_live), 'MM/DD/YYYY'),
                    TO_DATE(REGEXP_REPLACE(TRIM(proj.go_live), '(\d{4})(\d{2})(\d{2})', '\1-\2-\3'), 'YYYY-MM-DD')
                ),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden.kunden_nr AS "Account__c",
    chancen.chance_id AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON proj.kd = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON proj.opp = chancen.chance_id