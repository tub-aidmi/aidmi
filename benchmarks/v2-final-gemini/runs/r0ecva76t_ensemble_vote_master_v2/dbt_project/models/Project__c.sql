{{ config(materialized='table') }}

SELECT
    CAST('PRJ-' || p.projekt_kennung AS TEXT) AS "Id",
    COALESCE(p.projektname, p.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) = 'aktiv' THEN 'Active'
        WHEN LOWER(p.projektstatus) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(p.projektstatus) = 'in planung' THEN 'In Planning'
        WHEN LOWER(p.projektstatus) = 'angehalten' THEN 'On Hold'
        WHEN LOWER(p.projektstatus) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST('ACC-' || p.kunden_kennung AS TEXT) AS "Account__c",
    CAST('OPP-' || p.opp_kennung_ref AS TEXT) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p