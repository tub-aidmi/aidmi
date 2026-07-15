{{ config(materialized='table') }}

WITH projects AS (
    SELECT 
        p.projekt_kennung,
        p.projektname,
        p.projektstatus,
        p.go_live_datum,
        p.kunden_kennung,
        p.opp_kennung_ref,
        k.kundennummer AS account_src_key,
        o.opp_kennung AS opportunity_src_key
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
        ON TRIM(p.kunden_kennung) = TRIM(k.kundennummer)
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o 
        ON TRIM(p.opp_kennung_ref) = TRIM(o.opp_kennung)
)
SELECT
    CONCAT('P0XX', TRIM(projekt_kennung)) AS "Id",
    COALESCE(TRIM(projektname), 'Unnamed') AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planning' THEN 'In Planning'
        WHEN 'paused' THEN 'On Hold'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('A0XX', TRIM(account_src_key)) AS "Account__c",
    CONCAT('O0XX', TRIM(opportunity_src_key)) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM projects