{{ config(materialized='table') }}

WITH source AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref
    FROM {{ source('fixture_master_src', 'master_projekte') }}
)

SELECT
    CAST(projekt_kennung AS TEXT) AS "Id",
    CASE 
        WHEN TRIM(projektname) = '' OR projektname IS NULL THEN 'Unnamed Project'
        ELSE TRIM(projektname)
    END AS "Name",
    CASE INITCAP(TRIM(COALESCE(projektstatus, '')))
        WHEN 'Active' THEN 'Active'
        WHEN 'Aktiv' THEN 'Active'
        WHEN 'In Bearbeitung' THEN 'In Planning'
        WHEN 'Pending' THEN 'In Planning'
        WHEN 'Inactive' THEN 'Completed'
        WHEN 'Inaktiv' THEN 'Completed'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR TRIM(go_live_datum) = '' THEN NULL
        WHEN TRIM(go_live_datum) = 'N/A' OR TRIM(LOWER(go_live_datum)) = 'n/a' THEN NULL
        WHEN TRIM(go_live_datum) = '0000-00-00' THEN NULL
        -- ISO format YYYY-MM-DD
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            go_live_datum::TEXT
        -- German dot format DD.MM.YYYY
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
        -- US slash format MM/DD/YYYY (single digit month/day allowed)
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY')::TEXT
        -- Compact YYYYMMDD format
        WHEN go_live_datum ~ '^\d{8}$' THEN 
            TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(kunden_kennung AS TEXT) AS "Account__c",
    CAST(opp_kennung_ref AS TEXT) AS "Opportunity__c",
    CAST(projekt_kennung AS TEXT) AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM source;