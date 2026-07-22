{{ config(materialized='table') }}

SELECT
    "projekt_kennung" AS "Id",
    TRIM("projektname") AS "Name",
    CASE
        WHEN TRIM(LOWER("projektstatus")) IN ('aktiv', 'active') THEN 'Active'
        WHEN TRIM(LOWER("projektstatus")) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN TRIM(LOWER("projektstatus")) IN ('in planung', 'in planning') THEN 'In Planning'
        WHEN TRIM(LOWER("projektstatus")) IN ('in wartestellung', 'on hold') THEN 'On Hold'
        WHEN TRIM(LOWER("projektstatus")) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM("go_live_datum") ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("go_live_datum") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("go_live_datum") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("go_live_datum") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM("go_live_datum")
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = TRIM("kunden_kennung")),
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = "kunden_kennung")
    ) AS "Account__c",
    COALESCE(
        (SELECT "opp_kennung" FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} WHERE "opp_kennung" = TRIM("opp_kennung_ref")),
        (SELECT "opp_kennung" FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} WHERE "opp_kennung" = "opp_kennung_ref")
    ) AS "Opportunity__c",
    "projekt_kennung" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
