{{ config(materialized='table') }}

SELECT
    MD5(projekt_kennung) AS "Id",
    projektname AS "Name",
    CASE
        WHEN LOWER(projektstatus) = 'aktiv' THEN 'Active'
        WHEN LOWER(projektstatus) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(projektstatus) = 'in planung' THEN 'In Planning'
        WHEN LOWER(projektstatus) = 'auf eis gelegt' THEN 'On Hold'
        WHEN LOWER(projektstatus) = 'abgesagt' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL -- Explicitly handle the problematic sentinel date
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(opp_kennung_ref) AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}