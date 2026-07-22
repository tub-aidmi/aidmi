{{ config(materialized='table') }}

SELECT
    MD5(mp.projekt_kennung)::text AS "Id",
    COALESCE(INITCAP(TRIM(mp.projektname)), mp.projekt_kennung) AS "Name",
    CASE
        WHEN TRIM(UPPER(mp.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN TRIM(UPPER(mp.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN TRIM(UPPER(mp.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG') THEN 'In Planning'
        WHEN TRIM(UPPER(mp.projektstatus)) IN ('ON HOLD', 'ANGEHALTEN') THEN 'On Hold'
        WHEN TRIM(UPPER(mp.projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(mk.kundennummer)::text AS "Account__c",
    MD5(mo.opp_kennung)::text AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON mp.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    ON mp.opp_kennung_ref = mo.opp_kennung