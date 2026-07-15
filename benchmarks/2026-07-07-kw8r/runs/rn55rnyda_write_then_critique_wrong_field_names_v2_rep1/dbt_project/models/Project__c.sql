{{ config(materialized='table') }}

SELECT
    CAST(proj_id AS TEXT) AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(status)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(status)) IN ('completed', 'abgeschlossen', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(status)) IN ('in planning', 'planung', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(status)) IN ('on hold', 'pausiert', 'paused') THEN 'On Hold'
        WHEN LOWER(TRIM(status)) IN ('cancelled', 'storniert', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        WHEN go_live ~ '^\d{2}\.\d{2}(\.\d{4}|\.\d{2})?$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || LPAD(TRIM(COALESCE(c.kunden_nr, proj.kd)), 12, '0') AS "Account__c",
    NULL AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS c
    ON proj.kd = c.kunden_nr
WHERE proj_id IS NOT NULL AND TRIM(proj_id) != ''