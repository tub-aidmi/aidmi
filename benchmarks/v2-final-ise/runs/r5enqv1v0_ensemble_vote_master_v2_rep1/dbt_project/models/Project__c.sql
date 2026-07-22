{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(NULLIF(TRIM(mp.projektname), ''), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('completed', 'abgeschlossen', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('in planning', 'in planung', 'geplant') THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('cancelled', 'storniert', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mk.kundennummer AS "Account__c",
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mp.kunden_kennung = mk.kundennummer