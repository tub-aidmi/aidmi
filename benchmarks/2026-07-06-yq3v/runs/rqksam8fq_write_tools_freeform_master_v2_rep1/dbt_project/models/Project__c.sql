-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    MD5(projekte.projekt_kennung) AS "Id",
    projekte.projektname AS "Name",
    CASE UPPER(TRIM(projekte.projektstatus))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning' -- NOT NULL, sensible default
    END AS "Project_Status__c",
    CASE
        WHEN projekte.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(projekte.go_live_datum, 'YYYY-MM-DD')
        WHEN projekte.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(projekte.go_live_datum, 'DD.MM.YYYY')
        WHEN projekte.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(projekte.go_live_datum, 'MM/DD/YYYY')
        WHEN projekte.go_live_datum ~ '^\d{8}$' THEN TO_DATE(projekte.go_live_datum, 'YYYYMMDD')
        ELSE NULL -- Can be NULL, so NULL if unparseable
    END AS "Go_Live_Date__c",
    MD5(projekte.kunden_kennung) AS "Account__c",
    MD5(projekte.opp_kennung_ref) AS "Opportunity__c",
    projekte.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte