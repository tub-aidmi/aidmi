{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(INITCAP(projektname), 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'in bearbeitung' THEN 'Active'
        WHEN 'pending' THEN 'In Planning'
        WHEN 'inactive' THEN 'Cancelled'
        WHEN 'inaktiv' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' OR UPPER(TRIM(go_live_datum)) = 'N/A' THEN NULL
        WHEN TRIM(go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live_datum)
        WHEN TRIM(go_live_datum) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_projekte') }}