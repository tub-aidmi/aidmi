-- depends_on: {{ source('fixture_master_v2_src', 'master_projekte') }}
{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    projektname AS "Name",
    CASE
        WHEN LOWER(projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(projektstatus) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(projektstatus) = 'in planung' THEN 'In Planning'
        WHEN LOWER(projektstatus) = 'planung' THEN 'In Planning'
        WHEN LOWER(projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}