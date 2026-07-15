{{ config(materialized='table') }}

SELECT
    mp."projekt_kennung" AS "Id",
    TRIM(mp."projektname") AS "Name",
    CASE
        WHEN UPPER(TRIM(mp."projektstatus")) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM(mp."projektstatus")) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(mp."projektstatus")) IN ('IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(mp."projektstatus")) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(mp."projektstatus")) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp."go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' AND mp."go_live_datum" <> '0000-00-00' THEN mp."go_live_datum"
        WHEN mp."go_live_datum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp."go_live_datum" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mk."kundennummer" AS "Account__c",
    mo."opp_kennung" AS "Opportunity__c",
    mp."projekt_kennung" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON mp."kunden_kennung" = mk."kundennummer"
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
    ON mp."opp_kennung_ref" = mo."opp_kennung"