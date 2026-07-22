{{ config(materialized='table') }}

SELECT 
    projekt_kennung AS "Id",
    COALESCE(projektname, 'Unnamed Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) = 'in bearbeitung' THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) = 'pending' THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('inactive', 'inaktiv') THEN 'Completed'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) IN ('', 'N/A') THEN NULL
        WHEN go_live_datum = '0000-00-00' THEN NULL
        -- YYYYMMDD format (exactly 8 digits, no separators)
        WHEN go_live_datum ~ '^\d{8}$' THEN 
            SUBSTR(go_live_datum, 1, 4) || '-' || SUBSTR(go_live_datum, 5, 2) || '-' || SUBSTR(go_live_datum, 7, 2)
        -- YYYY-MM-DD format (already correct ISO format)
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        -- DD.MM.YYYY format (dot separator)
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        -- MM/DD/YYYY format (slash separator, single or double digit months/days)
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_projekte') }};