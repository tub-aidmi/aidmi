{{ config(materialized='table') }}

SELECT
    TRIM(projekt_kennung) AS "Id",
    COALESCE(TRIM(projektname), 'Unnamed Project ' || TRIM(projekt_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) = '0000-00-00' THEN NULL
        WHEN TRIM(go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(TRIM(go_live_datum) AS DATE), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(opp_kennung_ref) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
WHERE TRIM(projekt_kennung) IS NOT NULL
