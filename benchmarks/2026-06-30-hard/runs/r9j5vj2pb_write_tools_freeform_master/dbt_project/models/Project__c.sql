{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    projektname AS "Name",
    CASE
        WHEN UPPER(TRIM(projektstatus)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(projektstatus)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(projektstatus)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(projektstatus)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }}
