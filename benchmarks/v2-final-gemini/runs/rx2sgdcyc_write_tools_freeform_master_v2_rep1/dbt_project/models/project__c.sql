{{ config(materialized='table') }}

SELECT
    MD5(projekte.projekt_kennung) AS "Id",
    projekte.projektname AS "Name",
    CASE
        WHEN LOWER(projekte.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projekte.projektstatus) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(projekte.projektstatus) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(projekte.projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(projekte.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN projekte.go_live_datum = '0000-00-00' THEN NULL
        WHEN projekte.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(projekte.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN projekte.go_live_datum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(projekte.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN projekte.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(projekte.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN projekte.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(projekte.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kunden.kundennummer) AS "Account__c",
    MD5(opportunities.opp_kennung) AS "Opportunity__c",
    projekte.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    projekte.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opportunities
ON
    projekte.opp_kennung_ref = opportunities.opp_kennung
