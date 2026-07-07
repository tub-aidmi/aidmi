{{
    config(materialized='table')
}}

SELECT
    TRIM(mp.projekt_kennung) AS "Id",
    COALESCE(TRIM(mp.projektname), TRIM(mp.projekt_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'in planung' THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'auf eis' THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(mp.go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(mp.go_live_datum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c",
    TRIM(mp.opp_kennung_ref) AS "Opportunity__c",
    TRIM(mp.projekt_kennung) AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp