{{ config(materialized='table') }}

SELECT 
    MD5(projekt_kennung) AS "Id",
    TRIM(projektname) AS "Name",
    CASE 
        WHEN LOWER(TRIM(projektstatus)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('in planung', 'in planning', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('pausiert', 'on hold', 'pause') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('storniert', 'cancelled', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(opp_kennung_ref) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}