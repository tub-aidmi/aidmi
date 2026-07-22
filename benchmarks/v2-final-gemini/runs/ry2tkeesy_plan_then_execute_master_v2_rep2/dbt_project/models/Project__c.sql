-- dbt model for Project__c

{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(TRIM(p.projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.projektstatus)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.kunden_kennung AS "Account__c",
    p.opp_kennung_ref AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p