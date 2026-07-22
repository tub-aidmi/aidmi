{{ config(materialized='table') }}

SELECT
    SUBSTRING(MD5(mp.projekt_kennung), 1, 18) AS "Id",
    COALESCE(INITCAP(TRIM(mp.projektname)), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(mp.projektstatus))
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'AUF EIS' THEN 'On Hold'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    SUBSTRING(MD5(mk.kundennummer), 1, 18) AS "Account__c",
    SUBSTRING(MD5(mo.opp_kennung), 1, 18) AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON mp.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    ON mp.opp_kennung_ref = mo.opp_kennung
