{{ config(materialized='table') }}

SELECT
    mp."projekt_kennung" AS "Id",
    COALESCE(NULLIF(TRIM(mp."projektname"), ''), 'Untitled Project') AS "Name",
    CASE 
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('PAUSIERT', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN mp."go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mk."kundennummer" AS "Account__c",
    mo."opp_kennung" AS "Opportunity__c",
    mp."projekt_kennung" AS "Legacy_Project_ID__c",
    '2023-01-01'::text AS "CreatedDate",
    '2023-01-01'::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON TRIM(mp."kunden_kennung") = TRIM(mk."kundennummer")
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
    ON TRIM(mp."opp_kennung_ref") = TRIM(mo."opp_kennung")