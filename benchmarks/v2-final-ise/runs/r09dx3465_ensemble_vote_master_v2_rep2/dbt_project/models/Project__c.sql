{{ config(materialized='table') }}

SELECT
    'a00' || projekt_kennung AS "Id",
    INITCAP(TRIM(projektname)) AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'angehalten' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NOT NULL AND go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN go_live_datum IS NOT NULL AND go_live_datum ~ '^\d{8}$'
            THEN SUBSTR(go_live_datum, 1, 4) || '-' || SUBSTR(go_live_datum, 5, 2) || '-' || SUBSTR(go_live_datum, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || kunden_kennung AS "Account__c",
    '006' || opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}