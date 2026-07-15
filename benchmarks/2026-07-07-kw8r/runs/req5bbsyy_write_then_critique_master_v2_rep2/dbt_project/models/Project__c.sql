{{ config(materialized='table') }}

SELECT
    'P_' || REPLACE(p.projekt_kennung, 'PROJ-', '') AS "Id",
    INITCAP(TRIM(COALESCE(p.projektname, 'Unnamed Project'))) AS "Name",
    CASE
        WHEN LOWER(TRIM(p.projektstatus)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('in planning', 'in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00'
            THEN TO_DATE(p.go_live_datum, 'YYYY-MM-DD')::TEXT
        WHEN p.go_live_datum ~ '^\d{8}$'
            THEN TO_DATE(p.go_live_datum, 'YYYYMMDD')::TEXT
        WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_DATE(p.go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_DATE(p.go_live_datum, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE
        WHEN p.kunden_kennung ~ '\d+'
            THEN '001' || LPAD(REGEXP_REPLACE(p.kunden_kennung, '[^0-9]', ''), 9, '0')
        ELSE NULL
    END AS "Account__c",
    p.opp_kennung_ref AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p