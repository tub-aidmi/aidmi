{{ config(materialized='table') }}

SELECT
    '701' || SUBSTRING(MD5(p.projekt_kennung) FROM 1 FOR 15) AS "Id",
    COALESCE(NULLIF(TRIM(p.projektname), ''), 'Untitled Project') AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum IS NULL THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || SUBSTRING(MD5(c.kundennummer) FROM 1 FOR 15) AS "Account__c",
    CASE 
        WHEN o.opp_kennung IS NOT NULL THEN '006' || SUBSTRING(MD5(o.opp_kennung) FROM 1 FOR 15)
        ELSE NULL
    END AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON p.kunden_kennung = c.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p.opp_kennung_ref = o.opp_kennung
