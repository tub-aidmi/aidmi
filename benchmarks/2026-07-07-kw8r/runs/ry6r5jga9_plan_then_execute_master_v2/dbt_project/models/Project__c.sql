{{ config(materialized='table') }}

SELECT 
    'PROJ-' || TRIM(m.projekt_kennung) AS "Id",
    COALESCE(INITCAP(TRIM(m.projektname)), 'Unknown Project') AS "Name",
    CASE 
        WHEN TRIM(m.projektstatus) = 'In Arbeit' THEN 'Active'
        WHEN TRIM(m.projektstatus) = 'Fertig' THEN 'Completed'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE 
        WHEN m.go_live_datum IS NULL OR TRIM(m.go_live_datum) = '' THEN NULL
        WHEN m.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN m.go_live_datum
        WHEN m.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(m.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "Go_Live_Date__c",
    CASE WHEN k.kundennummer IS NOT NULL THEN 'CUST-' || TRIM(k.kundennummer) ELSE NULL END AS "Account__c",
    CASE WHEN m.opp_kennung_ref IS NOT NULL THEN 'OPP-' || TRIM(m.opp_kennung_ref) ELSE NULL END AS "Opportunity__c",
    TRIM(m.projekt_kennung) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} m
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON TRIM(m.kunden_kennung) = TRIM(k.kundennummer)