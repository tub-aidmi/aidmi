{{ config(materialized='table') }}

SELECT
    MD5(projekt.projekt_kennung) AS "Id",
    COALESCE(projekt.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN projekt.projektstatus ILIKE 'active' THEN 'Active'
        WHEN projekt.projektstatus ILIKE 'abgeschlossen' THEN 'Completed'
        WHEN projekt.projektstatus ILIKE 'in planung' OR projekt.projektstatus ILIKE 'planung' THEN 'In Planning'
        WHEN projekt.projektstatus ILIKE 'on hold' THEN 'On Hold'
        WHEN projekt.projektstatus ILIKE 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN projekt.go_live_datum = '0000-00-00' THEN NULL
        WHEN projekt.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(projekt.go_live_datum::DATE, 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN projekt.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(projekt.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(projekt.kunden_kennung) AS "Account__c",
    MD5(projekt.opp_kennung_ref) AS "Opportunity__c",
    projekt.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt