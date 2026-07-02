{{ config(materialized='table') }}

SELECT 
    CAST("projekt_kennung" AS TEXT) AS "Id",
    COALESCE(TRIM("projektname"), 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(COALESCE("projektstatus", '')))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'in bearbeitung' THEN 'Active'
        WHEN 'pending' THEN 'In Planning'
        WHEN 'inactive' THEN 'Cancelled'
        WHEN 'inaktiv' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(COALESCE("go_live_datum", '')) IN ('N/A', '0000-00-00') OR "go_live_datum" IS NULL THEN NULL
        WHEN "go_live_datum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "go_live_datum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "go_live_datum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("go_live_datum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "go_live_datum" ~ '^\d{4}-\d{2}-\d{2}$' THEN "go_live_datum"
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST("kunden_kennung" AS TEXT) AS "Account__c",
    CAST("opp_kennung_ref" AS TEXT) AS "Opportunity__c",
    CAST("projekt_kennung" AS TEXT) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_projekte') }}