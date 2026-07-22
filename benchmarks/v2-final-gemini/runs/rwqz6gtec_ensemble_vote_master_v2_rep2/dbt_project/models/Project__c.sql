{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(p.projektname, p.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(p.projektstatus) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(p.projektstatus) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(p.projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(p.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.kunden_kennung AS "Account__c",
    p.opp_kennung_ref AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p