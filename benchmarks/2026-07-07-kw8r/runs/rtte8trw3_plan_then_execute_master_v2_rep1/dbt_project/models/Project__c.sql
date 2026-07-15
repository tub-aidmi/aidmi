{{ config(materialized='table') }}

SELECT 
    INITCAP(TRIM(p.projekt_kennung)) AS "Id",
    INITCAP(TRIM(COALESCE(p.projektname, 'Unnamed Project'))) AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'ACTIV', 'IN PROGRESS', 'LAUFEND') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'FINISHED', 'BEENDET') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSED', 'PAUSIERT', 'HELD', 'GESTOPPT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT', 'TERMINATED', 'ABGELEHNT') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum IS NULL THEN NULL
        WHEN TRIM(p.go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live_datum)
        WHEN TRIM(p.go_live_datum) ~ '^\d{8}$' THEN 
            SUBSTR(TRIM(p.go_live_datum), 1, 4) || '-' || 
            SUBSTR(TRIM(p.go_live_datum), 5, 2) || '-' || 
            SUBSTR(TRIM(p.go_live_datum), 7, 2)
        WHEN TRIM(p.go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY')::TEXT
        ELSE NULL 
    END AS "Go_Live_Date__c",
    INITCAP(TRIM(m.kundennummer)) AS "Account__c",
    INITCAP(TRIM(p.opp_kennung_ref)) AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    '1900-01-01 00:00:00' AS "CreatedDate",
    '1900-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m 
    ON TRIM(p.kunden_kennung) = TRIM(m.kundennummer)