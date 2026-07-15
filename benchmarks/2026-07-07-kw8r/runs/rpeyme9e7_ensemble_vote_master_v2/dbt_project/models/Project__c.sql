{{ config(materialized='table') }}

SELECT
    '00P' || REPLACE(p."projekt_kennung", 'PROJ-', '') AS "Id",
    p."projektname" AS "Name",
    CASE
        WHEN UPPER(TRIM(p."projektstatus")) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live_datum"
        WHEN p."go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(p."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_datum" ~ '^\d{8}$' THEN
            TO_CHAR(TO_DATE(p."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p."go_live_datum" = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE
        WHEN p."kunden_kennung" IS NOT NULL THEN '001' || REPLACE(p."kunden_kennung", 'CUST-', '')
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN p."opp_kennung_ref" IS NOT NULL THEN '006' || REPLACE(p."opp_kennung_ref", 'OPP-', '')
        ELSE NULL
    END AS "Opportunity__c",
    p."projekt_kennung" AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p