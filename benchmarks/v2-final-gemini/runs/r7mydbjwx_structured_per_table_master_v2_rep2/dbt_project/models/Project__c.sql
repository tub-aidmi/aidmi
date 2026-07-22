{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(TRIM(projektname), '') AS "Name",
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projektstatus) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(projektstatus) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- Handles M/D/YYYY and MM/DD/YYYY
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