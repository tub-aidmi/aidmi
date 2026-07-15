{{ config(materialized='table') }}

SELECT
    CONCAT('00P', SUBSTRING(MD5(TRIM(p.projekt_kennung)), 1, 14)) AS "Id",
    COALESCE(TRIM(p.projektname), '') AS "Name",
    CASE LOWER(TRIM(COALESCE(p.projektstatus, '')))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'angehalten' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'completed' THEN 'Completed'
        WHEN 'active' THEN 'Active'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live_datum) IS NULL OR TRIM(p.go_live_datum) = '' THEN NULL
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live_datum)
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN
            SUBSTR(TRIM(p.go_live_datum), 1, 4) || '-' ||
            SUBSTR(TRIM(p.go_live_datum), 5, 2) || '-' ||
            SUBSTR(TRIM(p.go_live_datum), 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('001', SUBSTRING(MD5(TRIM(k.kundennummer)), 1, 15)) AS "Account__c",
    CONCAT('006', SUBSTRING(MD5(TRIM(p.opp_kennung_ref)), 1, 14)) AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(k.kundennummer) = TRIM(p.kunden_kennung)
WHERE TRIM(p.projekt_kennung) != ''