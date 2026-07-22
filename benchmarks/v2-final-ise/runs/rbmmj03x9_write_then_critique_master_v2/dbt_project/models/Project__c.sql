{{ config(materialized='table') }}
SELECT
    p."projekt_kennung" AS "Id",
    p."projektname" AS "Name",
    CASE
        WHEN LOWER(TRIM(p."projektstatus")) IN ('abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p."projektstatus")) IN ('cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p."go_live_datum" = '0000-00-00' THEN NULL
        WHEN p."go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live_datum"
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('ACCOUNT-', REPLACE(p."kunden_kennung", 'CUST-', '')) AS "Account__c",
    MD5(o."opp_kennung") AS "Opportunity__c",
    p."projekt_kennung" AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON p."kunden_kennung" = k."kundennummer"
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p."opp_kennung_ref" = o."opp_kennung"