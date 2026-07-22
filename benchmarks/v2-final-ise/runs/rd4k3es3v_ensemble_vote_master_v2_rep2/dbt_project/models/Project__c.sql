{{ config(materialized='table') }}

SELECT
    'proj_' || mp.projekt_kennung AS "Id",
    mp.projektname AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('aktiv') THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('pausiert', 'on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    'acc_' || mp.kunden_kennung AS "Account__c",
    'opp_' || mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp