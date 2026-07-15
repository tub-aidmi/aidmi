{{ config(materialized='table') }}

SELECT
    MD5(projekt_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(projektname), ''), 'Untitled Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE 
        WHEN kunden_kennung IS NOT NULL THEN MD5(kunden_kennung)
        ELSE NULL
    END AS "Account__c",
    CASE 
        WHEN opp_kennung_ref IS NOT NULL THEN MD5(opp_kennung_ref)
        ELSE NULL
    END AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
