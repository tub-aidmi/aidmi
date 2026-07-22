{{ config(materialized='table') }}

SELECT
    MD5(proj.projekt_kennung) AS "Id",
    COALESCE(TRIM(proj.projektname), 'Unknown Project') AS "Name",
    CASE LOWER(proj.projektstatus)
        WHEN 'aktiv' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'auf eis' THEN 'On Hold'
        WHEN 'abgesagt' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(proj.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN proj.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(MD5(kunden.kundennummer), MD5('UNKNOWN_ACCOUNT')) AS "Account__c",
    COALESCE(MD5(opps.opp_kennung), MD5('UNKNOWN_OPPORTUNITY')) AS "Opportunity__c",
    proj.projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS proj
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON proj.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opps
    ON proj.opp_kennung_ref = opps.opp_kennung
