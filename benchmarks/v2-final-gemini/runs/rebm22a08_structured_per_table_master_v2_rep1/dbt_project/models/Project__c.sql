-- depends_on: 

{{ config(materialized='table') }}

SELECT
    MD5(master_projekte.projekt_kennung) AS "Id",
    COALESCE(master_projekte.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(master_projekte.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(master_projekte.projektstatus) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(master_projekte.projektstatus) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(master_projekte.projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(master_projekte.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN master_projekte.go_live_datum = '0000-00-00' THEN NULL
        WHEN master_projekte.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(master_projekte.go_live_datum AS DATE), 'YYYY-MM-DD')
        WHEN master_projekte.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(master_projekte.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN master_projekte.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(master_projekte.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(master_projekte.kunden_kennung) AS "Account__c",
    MD5(master_projekte.opp_kennung_ref) AS "Opportunity__c",
    master_projekte.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS master_projekte