-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    projektname AS "Name",
    CASE
        WHEN projektstatus = 'Aktiv' THEN 'Active'
        WHEN projektstatus = 'Abgeschlossen' THEN 'Completed'
        WHEN projektstatus = 'In Planung' THEN 'In Planning'
        WHEN projektstatus = 'Ausgesetzt' THEN 'On Hold'
        WHEN projektstatus = 'Abgesagt' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum IS NULL OR go_live_datum = '' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}