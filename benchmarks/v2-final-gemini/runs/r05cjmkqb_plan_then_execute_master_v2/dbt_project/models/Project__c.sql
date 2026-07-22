-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(TRIM(p.projekt_kennung)) AS "Id",
    TRIM(p.projektname) AS "Name",
    CASE
        WHEN TRIM(UPPER(p.projektstatus)) = 'AKTIV' THEN 'Active'
        WHEN TRIM(UPPER(p.projektstatus)) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN TRIM(UPPER(p.projektstatus)) = 'IN PLANUNG' THEN 'In Planning'
        WHEN TRIM(UPPER(p.projektstatus)) = 'IN HALT' THEN 'On Hold'
        WHEN TRIM(UPPER(p.projektstatus)) = 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(TRIM(p.kunden_kennung)) AS "Account__c",
    MD5(TRIM(p.opp_kennung_ref)) AS "Opportunity__c",
    TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON TRIM(p.kunden_kennung) = TRIM(k.kundennummer)
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    ON TRIM(p.opp_kennung_ref) = TRIM(o.opp_kennung)