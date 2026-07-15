{{ config(materialized='table') }}

SELECT
    CAST(UPPER(TRIM(p.projekt_kennung)) AS TEXT) AS "Id",
    INITCAP(TRIM(p.projektname)) AS "Name",
    CASE
        WHEN UPPER(TRIM(p.projektstatus)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED', 'DONE', 'ERLEDIGT') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('PAUSIERT', 'ON HOLD', 'GESPERRT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ABBRECHEN', 'CANCELLED', 'GESTORBT', 'KILLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum IS NULL OR TRIM(p.go_live_datum) = '' THEN NULL
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_DATE(p.go_live_datum, 'YYYYMMDD')::TEXT
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live_datum, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(UPPER(TRIM(k.kundennummer)) AS TEXT) AS "Account__c",
    CAST(UPPER(TRIM(p.opp_kennung_ref)) AS TEXT) AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(p.kunden_kennung) = TRIM(k.kundennummer)
