-- depends_on: {{ source('fixture_master_v2_src', 'master_projekte') }}

{{ config(materialized='table') }}

SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(TRIM(p.projektname), 'No Project Name') AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) = 'aktiv' THEN 'Active'
        WHEN LOWER(p.projektstatus) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(p.projektstatus) = 'in planung' THEN 'In Planning'
        WHEN LOWER(p.projektstatus) = 'pausiert' THEN 'On Hold'
        WHEN LOWER(p.projektstatus) = 'abgebrochen' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum -- YYYY-MM-DD
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kunden_kennung) AS "Account__c",
    MD5(p.opp_kennung_ref) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p