{{ config(materialized='table') }}

SELECT
    'a1X' || UPPER(TRIM(projekt_kennung)) AS Id,
    COALESCE(
        NULLIF(INITCAP(TRIM(projektname)), ''),
        'Unknown Project'
    ) AS Name,
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('aktiv', 'active', 'in planung', 'planning') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed', 'beendet', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('geplant', 'planned', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('pausiert', 'on hold', 'gestoppt') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('storniert', 'cancelled', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) IS NULL OR go_live_datum = '' THEN NULL
        WHEN go_live_datum ~ '^0000-00-00$' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN (go_live_datum::DATE)::TEXT
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    'A00' || UPPER(TRIM(kunden_kennung)) AS "Account__c",
    '006' || UPPER(TRIM(opp_kennung_ref)) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    '2024-01-01' AS CreatedDate,
    '2024-01-01' AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}