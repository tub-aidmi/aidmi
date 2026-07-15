{{ config(materialized='table') }}

SELECT 
    CAST(mp.projekt_kennung AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(mp.projektname, ''))) AS "Name",
    CASE 
        WHEN LOWER(TRIM(COALESCE(mp.projektstatus, ''))) = 'in planung' THEN 'In Planning'
        WHEN LOWER(TRIM(COALESCE(mp.projektstatus, ''))) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(COALESCE(mp.projektstatus, ''))) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(TRIM(COALESCE(mp.projektstatus, ''))) = 'gesperrt' THEN 'On Hold'
        WHEN LOWER(TRIM(COALESCE(mp.projektstatus, ''))) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        ELSE NULL 
    END AS "Go_Live_Date__c",
    mk.kundennummer AS "Account__c",
    mo.opp_kennung AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
    ON TRIM(mp.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo 
    ON TRIM(mp.opp_kennung_ref) = TRIM(mo.opp_kennung)