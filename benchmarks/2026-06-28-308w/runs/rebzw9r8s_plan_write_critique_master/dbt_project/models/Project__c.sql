
{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(projektname, projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projektstatus) = 'in bearbeitung' THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('inactive', 'inaktiv') THEN 'Cancelled'
        WHEN LOWER(projektstatus) = 'pending' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CAST(go_live_datum AS TEXT) AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }}
