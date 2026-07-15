{{ config(materialized='table') }}

SELECT 
    INITCAP(TRIM(p.projekt_kennung)) AS "Id",
    INITCAP(TRIM(COALESCE(p.projektname, 'Unnamed Project'))) AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'ACTIV', 'IN PROGRESS', 'LAUFEND') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'FINISHED') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSED', 'PAUSIERT', 'HELD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT', 'TERMINATED') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum IS NULL THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{8}$' THEN SUBSTR(p.go_live_datum, 1, 4) || '-' || SUBSTR(p.go_live_datum, 5, 2) || '-' || SUBSTR(p.go_live_datum, 7, 2)
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live_datum, 'DD.MM.YYYY')::TEXT
        ELSE NULL 
    END AS "Go_Live_Date__c",
    acct."Id" AS "Account__c",
    opp."Id" AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    '1900-01-01 00:00:00' AS "CreatedDate",
    '1900-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ ref('Account') }} acct 
    ON INITCAP(TRIM(p.kunden_kennung)) = acct."Id"
LEFT JOIN {{ ref('Opportunity') }} opp 
    ON INITCAP(TRIM(p.opp_kennung_ref)) = opp."Id"