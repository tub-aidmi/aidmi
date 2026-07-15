{{ config(materialized='table') }}

SELECT
    '0PT' || SUBSTRING(MD5(projekt_kennung) FROM 1 FOR 15) AS "Id",
    projektname AS "Name",
    CASE 
        WHEN LOWER(TRIM(projektstatus)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) LIKE '%planung%' AND NOT LOWER(TRIM(projektstatus)) = 'aktiv' THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('angehalten', 'gesperrt') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('storniert', 'abgebrochen', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live_datum)
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || SUBSTRING(MD5(kunden_kennung) FROM 1 FOR 15) AS "Account__c",
    CASE 
        WHEN opp_kennung_ref IS NOT NULL AND TRIM(opp_kennung_ref) != '' THEN 
            '006' || SUBSTRING(MD5(opp_kennung_ref) FROM 1 FOR 15)
        ELSE NULL
    END AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}