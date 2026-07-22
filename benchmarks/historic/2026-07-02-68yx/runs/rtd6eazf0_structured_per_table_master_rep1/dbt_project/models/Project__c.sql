{{ config(materialized='table') }}

SELECT
    CAST(projekt_kennung AS TEXT) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) IN ('IN BEARBEITUNG') THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) IN ('PENDING') THEN 'In Planning'
        WHEN UPPER(TRIM(projektstatus)) IN ('INAKTIV', 'INACTIVE') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' OR TRIM(go_live_datum) = 'N/A' OR TRIM(go_live_datum) = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')::TEXT
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')::TEXT
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    CAST(projekt_kennung AS TEXT) AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_projekte') }}