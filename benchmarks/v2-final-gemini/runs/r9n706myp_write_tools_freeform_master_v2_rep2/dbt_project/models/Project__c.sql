{{
  config(materialized='table')
}}

SELECT
    MD5(TRIM(projekt_kennung)) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) = '0000-00-00' THEN NULL
        WHEN TRIM(go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY')
        WHEN TRIM(go_live_datum) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')
        ELSE NULL
    END::TEXT AS "Go_Live_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(opp_kennung_ref)) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}
