{{ config(materialized='table') }}

SELECT
    proj."proj_id" AS "Id",
    INITCAP(TRIM(proj."name")) AS "Name",
    CASE
        WHEN LOWER(TRIM(proj."status")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(proj."status")) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(proj."status")) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(proj."status")) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(proj."status")) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj."go_live" ~ '^\d{4}-\d{2}-\d{2}$' THEN proj."go_live"
        WHEN proj."go_live" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj."go_live", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj."go_live" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj."go_live", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden."kunden_nr" AS "Account__c",
    chancen."chance_id" AS "Opportunity__c",
    proj."proj_id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON proj."kd" = kunden."kunden_nr"
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON proj."opp" = chancen."chance_id"