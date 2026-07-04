-- This dbt model transforms source data into the Project__c target schema.
{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(mp.projektname, mp.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(mp.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(mp.projektstatus) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(mp.projektstatus) IN ('in planung', 'in planning', 'planung') THEN 'In Planning'
        WHEN LOWER(mp.projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(mp.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum = '0000-00-00' THEN NULL
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c",
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp