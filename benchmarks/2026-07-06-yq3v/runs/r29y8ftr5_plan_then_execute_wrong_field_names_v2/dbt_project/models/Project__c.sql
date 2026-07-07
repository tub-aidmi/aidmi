{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(proj.status)) = 'AKTIV' THEN 'Active'
        WHEN UPPER(TRIM(proj.status)) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM(proj.status)) = 'IN PLANUNG' THEN 'In Planning'
        WHEN UPPER(TRIM(proj.status)) = 'AUF EIS' THEN 'On Hold'
        WHEN UPPER(TRIM(proj.status)) = 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kunden.kunden_nr) AS "Account__c",
    MD5(chancen.chance_id) AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden ON proj.kd = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen ON proj.opp = chancen.chance_id
