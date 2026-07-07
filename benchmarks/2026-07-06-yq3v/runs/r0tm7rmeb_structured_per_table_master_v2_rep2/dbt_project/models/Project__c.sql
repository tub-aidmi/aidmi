-- dbt model for Project__c

{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(p.projektname, 'Unnamed Project') AS "Name",
    CASE
        WHEN p.projektstatus ILIKE 'active' OR p.projektstatus ILIKE 'aktiv' THEN 'Active'
        WHEN p.projektstatus ILIKE 'abgeschlossen' OR p.projektstatus ILIKE 'completed' THEN 'Completed'
        WHEN p.projektstatus ILIKE 'planung' OR p.projektstatus ILIKE 'in planung' OR p.projektstatus ILIKE 'in planning' THEN 'In Planning'
        WHEN p.projektstatus ILIKE 'on hold' OR p.projektstatus ILIKE 'pausiert' THEN 'On Hold'
        WHEN p.projektstatus ILIKE 'cancelled' OR p.projektstatus ILIKE 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum -- YYYY-MM-DD
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY or M/D/YYYY (TO_DATE is flexible enough for single digits)
        WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY or D.M.YYYY
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kunden_kennung) AS "Account__c",
    MD5(p.opp_kennung_ref) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p