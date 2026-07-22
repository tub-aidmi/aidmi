
{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(projektname, projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv', 'in bearbeitung') THEN 'Active'
        WHEN LOWER(projektstatus) IN ('inactive', 'inaktiv') THEN 'Completed'
        WHEN LOWER(projektstatus) = 'pending' THEN 'In Planning'
        ELSE 'In Planning' -- Fallback to a valid enum value as NULL is not allowed
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum -- Already in YYYY-MM-DD format
        ELSE NULL -- Handle N/A or other unparseable formats
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }}