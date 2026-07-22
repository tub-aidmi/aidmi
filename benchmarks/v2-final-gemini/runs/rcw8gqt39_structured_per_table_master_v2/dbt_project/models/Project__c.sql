{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    mp.projektname AS "Name",
    CASE
        WHEN mp.projektstatus ILIKE 'Aktiv' THEN 'Active'
        WHEN mp.projektstatus ILIKE 'Abgeschlossen' THEN 'Completed'
        WHEN mp.projektstatus ILIKE 'In Planung' THEN 'In Planning'
        WHEN mp.projektstatus ILIKE 'Pausiert' THEN 'On Hold'
        WHEN mp.projektstatus ILIKE 'Abgebrochen' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c",
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
