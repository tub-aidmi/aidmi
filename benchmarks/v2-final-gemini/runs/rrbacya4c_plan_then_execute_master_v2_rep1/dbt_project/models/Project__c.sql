{{ config(materialized='table') }}

SELECT
    gen_random_uuid() AS "Id",
    COALESCE(TRIM(projekt.projektname), 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(projekt.projektstatus))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in_planung' THEN 'In Planning'
        WHEN 'auf_eis' THEN 'On Hold'
        WHEN 'abgebrochen' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(projekt.go_live_datum) ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(projekt.go_live_datum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(TRIM(projekt.kunden_kennung)) AS "Account__c",
    MD5(TRIM(projekt.opp_kennung_ref)) AS "Opportunity__c",
    TRIM(projekt.projekt_kennung) AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt
