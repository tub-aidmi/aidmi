-- This dbt model transforms project data from the source system into the Project__c target schema.
{{ config(materialized='table') }}

SELECT
    MD5(projekt.projekt_kennung) AS "Id",
    projekt.projektname AS "Name",
    CASE
        WHEN LOWER(projekt.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projekt.projektstatus) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(projekt.projektstatus) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(projekt.projektstatus) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(projekt.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN projekt.go_live_datum = '0000-00-00' THEN NULL
        WHEN projekt.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(projekt.kunden_kennung) AS "Account__c",
    MD5(projekt.opp_kennung_ref) AS "Opportunity__c",
    projekt.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt