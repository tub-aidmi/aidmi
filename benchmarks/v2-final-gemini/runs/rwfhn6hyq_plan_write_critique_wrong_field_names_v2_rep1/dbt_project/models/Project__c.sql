{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project Name') AS "Name",
    CASE UPPER(proj.status)
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'ANGEHALTEN' THEN 'On Hold'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(proj.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TO_CHAR(TO_DATE(TRIM(proj.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE
            NULL
    END AS "Go_Live_Date__c",
    kunden.kunden_nr AS "Account__c",
    chancen.chance_id AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON TRIM(proj.kd) = TRIM(kunden.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON TRIM(proj.opp) = TRIM(chancen.chance_id)
