
{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(NULLIF(TRIM(p.projektname), ''), 'Untitled Project - ' || p.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(p.projektstatus) IN ('inactive', 'inaktiv') THEN 'Cancelled'
        WHEN LOWER(p.projektstatus) IN ('in bearbeitung', 'pending') THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kundennummer AS "Account__c",
    o.opp_kennung AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS k ON p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_src', 'master_opportunities') }} AS o ON p.opp_kennung_ref = o.opp_kennung
