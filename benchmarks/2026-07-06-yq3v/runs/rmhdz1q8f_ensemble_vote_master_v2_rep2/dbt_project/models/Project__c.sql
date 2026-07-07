{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(TRIM(projektname), 'Unnamed Project ' || projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('in planung', 'planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(go_live_datum AS DATE)::TEXT
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}