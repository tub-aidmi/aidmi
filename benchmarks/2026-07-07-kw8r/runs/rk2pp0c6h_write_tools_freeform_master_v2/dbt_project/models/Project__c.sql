{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        p.projekt_kennung,
        p.projektname,
        p.projektstatus,
        p.go_live_datum,
        p.kunden_kennung,
        p.opp_kennung_ref,
        c.kundennummer AS account_kundennummer,
        o.opp_kennung AS opportunity_opp_kennung
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
        ON p.kunden_kennung = c.kundennummer
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o 
        ON p.opp_kennung_ref = o.opp_kennung
)

SELECT
    projekt_kennung AS "Id",
    projektname AS "Name",
    CASE 
        WHEN UPPER(TRIM(projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(projektstatus)) IN ('IN PLANNING', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(projektstatus)) IN ('ON HOLD', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account_kundennummer AS "Account__c",
    opportunity_opp_kennung AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM project_data
