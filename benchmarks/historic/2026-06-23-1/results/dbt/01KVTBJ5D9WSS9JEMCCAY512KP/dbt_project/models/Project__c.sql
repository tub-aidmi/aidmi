{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS Id,
    mp.projektname AS Name,
    CASE
        WHEN mp.projektstatus = 'In Bearbeitung' THEN 'In Planning'
        WHEN mp.projektstatus = 'Active' THEN 'Active'
        WHEN mp.projektstatus = 'Inactive' THEN 'On Hold'
        WHEN mp.projektstatus = 'Completed' THEN 'Completed'
        WHEN mp.projektstatus = 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS Project_Status__c,
    mp.go_live_datum AS Go_Live_Date__c,
    mp.kunden_kennung AS Account__c,
    mp.opp_kennung_ref AS Opportunity__c,
    mp.projekt_kennung AS Legacy_Project_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted
FROM
    {{ source('fixture_master_src', 'master_projekte') }} mp