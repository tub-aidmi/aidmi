{{ config(materialized='table') }}

SELECT 
    -- Generate Salesforce-style Id for custom object (0U1 prefix)
    CONCAT('0U1', LEFT(MD5(TRIM(proj_id)), 12)) AS "Id",
    TRIM(name) AS "Name",
    -- Map German status values to English enum; fallback NULL
    CASE LOWER(TRIM(status))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'angehalten' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'gesperrt' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    -- Parse DD.MM.YYYY or YYYY-MM-DD dates; return NULL for invalid/missing
    CASE 
        WHEN go_live IS NOT NULL AND TRIM(go_live) <> '' AND go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live), 'DD.MM.YYYY')::TEXT
        WHEN go_live IS NOT NULL AND TRIM(go_live) <> '' AND go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN SUBSTRING(TRIM(go_live) FROM 1 FOR 10)
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account__c: generate SF-style Account Id from source customer number (kd)
    CASE 
        WHEN TRIM(kd) IS NOT NULL AND TRIM(kd) <> '' THEN CONCAT('0A0', LEFT(MD5(TRIM(kd)), 12))
        ELSE NULL
    END AS "Account__c",
    -- Opportunity__c: generate SF-style Opportunity Id from source opp id (opp)
    CASE 
        WHEN TRIM(opp) IS NOT NULL AND TRIM(opp) <> '' THEN CONCAT('006', LEFT(MD5(TRIM(opp)), 12))
        ELSE NULL
    END AS "Opportunity__c",
    -- Legacy key for row-level traceability
    TRIM(proj_id) AS "Legacy_Project_ID__c",
    -- Audit placeholders (source lacks timestamps; use ISO date string per spec)
    '2024-01-01'::TEXT AS "CreatedDate",
    '2024-01-01'::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
WHERE TRIM(proj_id) IS NOT NULL AND TRIM(proj_id) <> ''