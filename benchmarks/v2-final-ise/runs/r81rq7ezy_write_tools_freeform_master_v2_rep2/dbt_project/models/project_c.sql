{{ config(materialized='table') }}

SELECT
    'P' || SUBSTR(MD5(src."projekt_kennung"), 1, 14) AS "Id",
    src."projektname" AS "Name",
    CASE
        WHEN UPPER(TRIM(src."projektstatus")) = 'AKTIV' THEN 'Active'
        WHEN UPPER(TRIM(src."projektstatus")) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM(src."projektstatus")) = 'IN PLANUNG' THEN 'In Planning'
        WHEN UPPER(TRIM(src."projektstatus")) = 'AUSGESETZT' THEN 'On Hold'
        WHEN UPPER(TRIM(src."projektstatus")) = 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src."go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src."go_live_datum" ~ '^\d{8}$' THEN TO_CHAR(
            TO_DATE(SUBSTR(src."go_live_datum", 1, 4) || '-' || SUBSTR(src."go_live_datum", 5, 2) || '-' || SUBSTR(src."go_live_datum", 7, 2), 'YYYY-MM-DD'), 'YYYY-MM-DD'
        )
        ELSE NULL
    END AS "Go_Live_Date__c",
    'A' || SUBSTR(MD5(src."kunden_kennung"), 1, 14) AS "Account__c",
    'O' || SUBSTR(MD5(src."opp_kennung_ref"), 1, 14) AS "Opportunity__c",
    src."projekt_kennung" AS "Legacy_Project_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} src
