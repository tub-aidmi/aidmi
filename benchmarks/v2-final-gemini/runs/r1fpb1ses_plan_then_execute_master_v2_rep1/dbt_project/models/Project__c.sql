-- This dbt model transforms data from master_projekte, master_kunden, and master_opportunities
-- into the Project__c target table.

{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung)::TEXT AS "Id",
    COALESCE(TRIM(p.projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.projektstatus)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('angehalten', 'on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.projektstatus)) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kundennummer)::TEXT AS "Account__c",
    MD5(o.opp_kennung)::TEXT AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k ON p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o ON p.opp_kennung_ref = o.opp_kennung;