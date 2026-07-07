{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(p.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('PLANUNG', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kunden_kennung) AS "Account__c",
    MD5(p.opp_kennung_ref) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p