{{ config(materialized='table') }}

SELECT
    TRIM(projekt_kennung) AS "Id",
    CASE
        WHEN TRIM(projektname) IS NULL OR TRIM(projektname) = '' THEN 'Unnamed Project'
        ELSE INITCAP(TRIM(projektname))
    END AS "Name",
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('aktiv', 'active', 'in arbeit') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed', 'fertig', 'beendet') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('planung', 'in planning', 'angelegt', 'entwurf') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('pausiert', 'on hold', 'gestoppt', 'angehalten') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('storniert', 'cancelled', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) IS NULL OR TRIM(go_live_datum) = '' OR TRIM(go_live_datum) = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live_datum)
        WHEN go_live_datum ~ '^\d{8}$' THEN
            SUBSTR(TRIM(go_live_datum), 1, 4) || '-' ||
            SUBSTR(TRIM(go_live_datum), 5, 2) || '-' ||
            SUBSTR(TRIM(go_live_datum), 7, 2)
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(opp_kennung_ref) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }}