{{ config(materialized='table') }}

SELECT
    md5(p."projekt_kennung") AS "Id",
    p."projektname" AS "Name",
    CASE
        WHEN LOWER(TRIM(p."projektstatus")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('on hold', 'angehalten') THEN 'On Hold'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live_datum"
        WHEN p."go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_datum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    md5(k."kundennummer") AS "Account__c",
    md5(o."opp_kennung") AS "Opportunity__c",
    p."projekt_kennung" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON p."kunden_kennung" = k."kundennummer"
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p."opp_kennung_ref" = o."opp_kennung"