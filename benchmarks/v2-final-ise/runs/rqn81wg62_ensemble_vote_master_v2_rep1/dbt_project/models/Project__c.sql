{{ config(materialized='table') }}

SELECT
    CAST(p.projekt_kennung AS TEXT) AS "Id",
    COALESCE(TRIM(p.projektname), 'Unnamed Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.projektstatus)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('in planning', 'in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum IS NULL OR TRIM(p.go_live_datum) = '' THEN NULL
        WHEN TRIM(p.go_live_datum) = '0000-00-00' THEN NULL
        WHEN TRIM(p.go_live_datum) ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN 
            TO_CHAR(CAST(TRIM(p.go_live_datum) AS DATE), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(k.kundennummer AS TEXT) AS "Account__c",
    CAST(o.opp_kennung AS TEXT) AS "Opportunity__c",
    CAST(p.projekt_kennung AS TEXT) AS "Legacy_Project_ID__c",
    CAST('' AS TEXT) AS "CreatedDate",
    CAST('' AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o 
    ON p.opp_kennung_ref = o.opp_kennung