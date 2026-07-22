{{ config(materialized='table') }}

SELECT
    CAST(projekt_kennung AS TEXT) AS "Id",
    CASE
        WHEN TRIM(COALESCE(projektname, '')) = '' THEN 'Unnamed Project'
        ELSE INITCAP(TRIM(projektname))
    END AS "Name",
    CASE
        WHEN UPPER(TRIM(COALESCE(projektstatus, ''))) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(COALESCE(projektstatus, ''))) = 'IN BEARBEITUNG' THEN 'Active'
        WHEN UPPER(TRIM(COALESCE(projektstatus, ''))) IN ('PENDING', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(COALESCE(projektstatus, ''))) IN ('INACTIVE', 'INAKTIV') THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        -- YYYY-MM-DD (already ISO)
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_datum != '0000-00-00' THEN go_live_datum
        -- YYYYMMDD
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        -- DD.MM.YYYY
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- MM/DD/YYYY (single or double digit month/day)
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(kunden_kennung AS TEXT) AS "Account__c",
    CAST(opp_kennung_ref AS TEXT) AS "Opportunity__c",
    CAST(projekt_kennung AS TEXT) AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_projekte') }}