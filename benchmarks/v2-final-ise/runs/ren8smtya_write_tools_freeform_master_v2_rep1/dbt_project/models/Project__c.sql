{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        kundennummer,
        'ACCT-' || LPAD(ROW_NUMBER() OVER (ORDER BY kundennummer)::TEXT, 6, '0') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
opportunity_mapping AS (
    SELECT 
        opp_kennung,
        'OPP-' || LPAD(ROW_NUMBER() OVER (ORDER BY opp_kennung)::TEXT, 6, '0') AS opportunity_id
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
    'PROJ-' || LPAD(ROW_NUMBER() OVER (ORDER BY p.projekt_kennung)::TEXT, 6, '0') AS "Id",
    p.projektname AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'PLANUNG', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    am.account_id AS "Account__c",
    om.opportunity_id AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN account_mapping am ON p.kunden_kennung = am.kundennummer
LEFT JOIN opportunity_mapping om ON p.opp_kennung_ref = om.opp_kennung
