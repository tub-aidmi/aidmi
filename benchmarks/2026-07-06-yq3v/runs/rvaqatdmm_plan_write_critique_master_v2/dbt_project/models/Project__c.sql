-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(TRIM(p.projektname), 'Unknown Project ' || p.projekt_kennung) AS "Name",
    CASE
        WHEN TRIM(LOWER(p.projektstatus)) = 'active' THEN 'Active'
        WHEN TRIM(LOWER(p.projektstatus)) = 'completed' THEN 'Completed'
        WHEN TRIM(LOWER(p.projektstatus)) = 'in planning' THEN 'In Planning'
        WHEN TRIM(LOWER(p.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(p.projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum IS NULL THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum -- Already YYYY-MM-DD
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' AND TO_DATE(p.go_live_datum, 'DD.MM.YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}\d{2}\d{2}$' AND TO_DATE(p.go_live_datum, 'YYYYMMDD') IS NOT NULL THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' AND TO_DATE(p.go_live_datum, 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kundennummer) AS "Account__c",
    MD5(o.opp_kennung) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    ON p.opp_kennung_ref = o.opp_kennung