{{ config(materialized='table') }}
SELECT 
    MD5(mp."projekt_kennung") AS "Id",
    TRIM(mp."projektname") AS "Name",
    CASE 
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('in planung', 'in planning', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN mp."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN mp."go_live_datum"
        WHEN mp."go_live_datum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" = '0000-00-00' THEN NULL
        ELSE NULL 
    END AS "Go_Live_Date__c",
    MD5(mp."kunden_kennung") AS "Account__c",
    MD5(mp."opp_kennung_ref") AS "Opportunity__c",
    mp."projekt_kennung" AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp