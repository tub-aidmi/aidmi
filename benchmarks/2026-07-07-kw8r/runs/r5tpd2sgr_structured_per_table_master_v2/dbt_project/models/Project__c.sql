{{ config(materialized='table') }}

SELECT
    p."projekt_kennung" AS "Id",
    p."projektname" AS "Name",
    CASE 
        WHEN UPPER(TRIM(p."projektstatus")) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('PAUSIERT', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p."projektstatus")) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live_datum"
        WHEN p."go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_datum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p."kunden_kennung" AS "Account__c",
    p."opp_kennung_ref" AS "Opportunity__c",
    p."projekt_kennung" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p