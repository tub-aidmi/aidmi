{{
    config(materialized='table')
}}

SELECT
    projekt.projekt_kennung AS "Id",
    COALESCE(TRIM(projekt.projektname), projekt.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(projekt.projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(projekt.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(projekt.projektstatus)) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(projekt.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(projekt.projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN projekt.go_live_datum = '0000-00-00' THEN NULL
        WHEN projekt.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN projekt.go_live_datum
        WHEN projekt.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(kunden.kundennummer) AS "Account__c",
    TRIM(mo.opp_kennung) AS "Opportunity__c",
    projekt.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    projekt.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
ON
    projekt.opp_kennung_ref = mo.opp_kennung