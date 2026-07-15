{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

account_mapping AS (
    SELECT
        kundennummer AS "AccountId",
        kundennummer AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_mapping AS (
    SELECT
        opp_kennung AS "OpportunityId",
        opp_kennung AS "Legacy_Opportunity_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
    p.projekt_kennung AS "Id",
    p.projektname AS "Name",
    CASE
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum IS NULL THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    am."AccountId" AS "Account__c",
    om."OpportunityId" AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM project_data p
LEFT JOIN account_mapping am ON p.kunden_kennung = am."Legacy_Customer_ID__c"
LEFT JOIN opportunity_mapping om ON p.opp_kennung_ref = om."Legacy_Opportunity_ID__c"
