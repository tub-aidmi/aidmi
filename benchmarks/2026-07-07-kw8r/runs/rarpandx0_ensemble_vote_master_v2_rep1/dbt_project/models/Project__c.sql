{{ config(materialized='table') }}

SELECT
    mp."projekt_kennung" AS "Id",
    mp."projektname" AS "Name",
    CASE
        WHEN LOWER(TRIM(mp."projektstatus")) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(mp."projektstatus")) IN ('in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(mp."projektstatus")) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(mp."projektstatus")) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            CASE WHEN mp."go_live_datum" = '0000-00-00' THEN NULL ELSE mp."go_live_datum" END
        WHEN mp."go_live_datum" ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(mp."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(mp."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(mp."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp."kunden_kennung" AS "Account__c",
    mp."opp_kennung_ref" AS "Opportunity__c",
    mp."projekt_kennung" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp