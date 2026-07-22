{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
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
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum <> '0000-00-00' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kundennummer) AS "Account__c",
    MD5(o.opp_kennung) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    ON p.opp_kennung_ref = o.opp_kennung
