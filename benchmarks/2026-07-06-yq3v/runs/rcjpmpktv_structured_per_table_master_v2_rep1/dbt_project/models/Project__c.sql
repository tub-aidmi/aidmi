-- depends_on: {{ source('fixture_master_v2_src', 'master_projekte') }}
{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(TRIM(mp.projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'in planung' THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c",
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp