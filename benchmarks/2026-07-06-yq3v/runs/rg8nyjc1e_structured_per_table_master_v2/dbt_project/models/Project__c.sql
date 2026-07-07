{{ config(materialized='table') }}

SELECT
    TRIM(mp.projekt_kennung) AS "Id",
    COALESCE(TRIM(mp.projektname), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN LENGTH(TRIM(mp.go_live_datum)) = 10 AND SUBSTRING(TRIM(mp.go_live_datum), 5, 1) = '-' AND SUBSTRING(TRIM(mp.go_live_datum), 8, 1) = '-' THEN
            TO_CHAR(TO_DATE(TRIM(mp.go_live_datum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN LENGTH(TRIM(mp.go_live_datum)) = 8 AND TRIM(mp.go_live_datum) ~ '^\d{8}$' THEN
            TO_CHAR(TO_DATE(TRIM(mp.go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN LENGTH(TRIM(mp.go_live_datum)) >= 8 AND LENGTH(TRIM(mp.go_live_datum)) <= 10 AND TRIM(mp.go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(mp.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN LENGTH(TRIM(mp.go_live_datum)) = 10 AND SUBSTRING(TRIM(mp.go_live_datum), 3, 1) = '.' AND SUBSTRING(TRIM(mp.go_live_datum), 6, 1) = '.' THEN
            TO_CHAR(TO_DATE(TRIM(mp.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE
            NULL
    END AS "Go_Live_Date__c",
    TRIM(mk.kundennummer) AS "Account__c",
    TRIM(mo.opp_kennung) AS "Opportunity__c",
    TRIM(mp.projekt_kennung) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON TRIM(mp.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    ON TRIM(mp.opp_kennung_ref) = TRIM(mo.opp_kennung);
