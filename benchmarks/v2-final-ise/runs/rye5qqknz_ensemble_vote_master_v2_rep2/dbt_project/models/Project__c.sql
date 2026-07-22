{{ config(materialized='table') }}

SELECT 
    'PROJ-' || p.projekt_kennung AS "Id",
    p.projektname AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('in planung', 'in planning', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('cancelled', 'storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE 
        WHEN k.kundennummer IS NOT NULL THEN 'ACCT-' || k.kundennummer
        ELSE NULL
    END AS "Account__c",
    CASE 
        WHEN o.opp_kennung IS NOT NULL THEN 'OPP-' || o.opp_kennung
        ELSE NULL
    END AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k ON p.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o ON p.opp_kennung_ref = o.opp_kennung