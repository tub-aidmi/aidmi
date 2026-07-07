-- models/Project__c.sql
{{ config(materialized='table') }}

SELECT
    MD5(projekte.projekt_kennung) AS "Id",
    COALESCE(projekte.projektname, projekte.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(projekte.projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(projekte.projektstatus) = 'completed' THEN 'Completed'
        WHEN LOWER(projekte.projektstatus) = 'in planning' THEN 'In Planning'
        WHEN LOWER(projekte.projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(projekte.projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    (CASE
        WHEN projekte.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(projekte.go_live_datum, 'DD.MM.YYYY')
        WHEN projekte.go_live_datum ~ '^\d{8}$' THEN TO_DATE(projekte.go_live_datum, 'YYYYMMDD')
        WHEN projekte.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(projekte.go_live_datum, 'MM/DD/YYYY')
        ELSE NULL
    END)::text AS "Go_Live_Date__c",
    MD5(projekte.kunden_kennung) AS "Account__c",
    MD5(projekte.opp_kennung_ref) AS "Opportunity__c",
    projekte.projekt_kennung AS "Legacy_Project_ID__c",
    NOW()::text AS "CreatedDate",
    NOW()::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
