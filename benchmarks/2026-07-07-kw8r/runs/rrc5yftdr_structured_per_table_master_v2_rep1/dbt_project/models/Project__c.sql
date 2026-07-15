{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    TRIM(p.projektname) AS "Name",
    CASE LOWER(TRIM(p.projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum IS NULL THEN NULL
        WHEN LOWER(TRIM(p.go_live_datum)) IN ('n/a', 'none', '') THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00'
            THEN TO_DATE(p.go_live_datum, 'YYYY-MM-DD')::TEXT
        WHEN p.go_live_datum ~ '^\d{8}$'
            THEN TO_DATE(p.go_live_datum, 'YYYYMMDD')::TEXT
        WHEN p.go_live_datum ~ '^\d+\.\d+\.\d{4}$'
            THEN TO_DATE(p.go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live_datum ~ '^\d+/\d+/\d{4}$'
            THEN TO_DATE(p.go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    m.kundennummer AS "Account__c",
    o.opp_kennung AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m
    ON p.kunden_kennung = m.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p.opp_kennung_ref = o.opp_kennung