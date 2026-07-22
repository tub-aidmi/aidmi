{{ config(materialized='table') }}

SELECT 
    COALESCE(TRIM(projekt_kennung), 'UNKNOWN') AS "Id",
    COALESCE(NULLIF(TRIM(INITCAP(projektname)), ''), 'Untitled Project') AS "Name",
    CASE LOWER(TRIM(COALESCE(projektstatus, '')))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planning' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_datum, 'YYYY-MM-DD')::TEXT
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    UPPER(TRIM(REGEXP_REPLACE(COALESCE(kunden_kennung, ''), '^(KUN-|CUST-)', '', 'i'))) AS "Account__c",
    TRIM(opp_kennung_ref) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}