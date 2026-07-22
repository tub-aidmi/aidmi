{{ config(materialized='table') }}

SELECT
    CAST(projekt_kennung AS TEXT) AS "Id",
    CASE
        WHEN TRIM(projektname) IS NULL OR TRIM(projektname) = '' THEN 'Unnamed Project'
        ELSE INITCAP(TRIM(projektname))
    END AS "Name",
    CASE
        WHEN LOWER(TRIM(COALESCE(projektstatus, ''))) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(COALESCE(projektstatus, ''))) = 'in bearbeitung' THEN 'In Planning'
        WHEN LOWER(TRIM(COALESCE(projektstatus, ''))) = 'pending' THEN 'In Planning'
        WHEN LOWER(TRIM(COALESCE(projektstatus, ''))) IN ('inactive', 'inaktiv') THEN 'Completed'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN UPPER(TRIM(go_live_datum)) = 'N/A' THEN NULL
        WHEN TRIM(go_live_datum) = '0000-00-00' THEN NULL
        WHEN TRIM(go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(go_live_datum) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(kunden_kennung AS TEXT) AS "Account__c",
    CAST(opp_kennung_ref AS TEXT) AS "Opportunity__c",
    CAST(projekt_kennung AS TEXT) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_projekte') }}