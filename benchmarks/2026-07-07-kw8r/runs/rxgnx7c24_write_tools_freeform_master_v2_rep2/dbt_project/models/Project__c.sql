{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(p.projektname, 'Untitled Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT', 'STORNO') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum IS NULL OR p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        ELSE NULL
    END AS "Go_Live_Date__c",
    mk.kundennummer AS "Account__c",
    mo.opp_kennung AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON p.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
    ON p.opp_kennung_ref = mo.opp_kennung
