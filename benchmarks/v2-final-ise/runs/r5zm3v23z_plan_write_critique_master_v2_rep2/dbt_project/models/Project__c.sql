{{ config(materialized='table') }}

SELECT
    'a2X' || MD5(p.projekt_kennung) AS "Id",
    COALESCE(INITCAP(TRIM(p.projektname)), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.projektstatus)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('pausiert', 'on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_DATE(p.go_live_datum, 'YYYYMMDD')::TEXT
        WHEN p.go_live_datum ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(p.go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    'A' || MD5(k.kundennummer) AS "Account__c",
    '006' || MD5(o.opp_kennung) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p.opp_kennung_ref = o.opp_kennung