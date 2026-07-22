{{ config(materialized='table') }}
WITH project_data AS (
    SELECT
        mp."projekt_kennung",
        mp."projektname",
        mp."projektstatus",
        mp."go_live_datum",
        mp."kunden_kennung",
        mp."opp_kennung_ref"
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
),
account_join AS (
    SELECT
        p."projekt_kennung",
        p."projektname",
        p."projektstatus",
        p."go_live_datum",
        p."kunden_kennung",
        p."opp_kennung_ref",
        mk."kundennummer" AS account_kundennummer
    FROM project_data p
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON TRIM(p."kunden_kennung") = TRIM(mk."kundennummer")
        AND p."kunden_kennung" LIKE 'CUST-%'
),
opportunity_join AS (
    SELECT
        a.*,
        mo."opp_kennung" AS opportunity_opp_kennung
    FROM account_join a
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
        ON TRIM(REPLACE(a."opp_kennung_ref", 'OPP-M-', 'OPP-')) = TRIM(mo."opp_kennung")
)
SELECT
    MD5(o."projekt_kennung") AS "Id",
    INITCAP(TRIM(o."projektname")) AS "Name",
    CASE
        WHEN UPPER(TRIM(o."projektstatus")) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(o."projektstatus")) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(o."projektstatus")) IN ('IN PLANUNG', 'PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(o."projektstatus")) IN ('ON HOLD', 'HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(o."projektstatus")) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(o."go_live_datum") ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o."go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(o."go_live_datum") ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o."go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(o."go_live_datum") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o."go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(o."go_live_datum") ~ '^\d{4}-\d{2}-\d{2}$' AND o."go_live_datum" != '0000-00-00' THEN o."go_live_datum"
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE WHEN o.account_kundennummer IS NOT NULL THEN MD5(o.account_kundennummer) ELSE NULL END AS "Account__c",
    CASE WHEN o.opportunity_opp_kennung IS NOT NULL THEN MD5(o.opportunity_opp_kennung) ELSE NULL END AS "Opportunity__c",
    o."projekt_kennung" AS "Legacy_Project_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_join o