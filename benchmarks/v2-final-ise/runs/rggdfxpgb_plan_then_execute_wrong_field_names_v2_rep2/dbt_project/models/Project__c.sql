{{ config(materialized='table') }}

SELECT 
    CONCAT('a0N', TRIM(p.proj_id)) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(p.name)), ''), 'Unknown Project') AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.status)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.status)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'ERLEDIGT') THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) IN ('IN PLANNING', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) IN ('ON HOLD', 'PAUSE') THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live)
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE 
        WHEN k.kunden_nr IS NOT NULL THEN CONCAT('a00', TRIM(k.kunden_nr))
        ELSE NULL 
    END AS "Account__c",
    CASE 
        WHEN c.chance_id IS NOT NULL THEN CONCAT('a06', TRIM(c.chance_id))
        ELSE NULL 
    END AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON TRIM(p.opp) = TRIM(c.chance_id)