{{ config(materialized='table') }}

SELECT
    projekt_kennung AS Id,
    projektname AS Name,
    CASE 
        WHEN LOWER(projektstatus) IN ('active', 'in bearbeitung') THEN 'Active'
        WHEN LOWER(projektstatus) = 'completed' THEN 'Completed'
        WHEN LOWER(projektstatus) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('on hold', 'inactive') THEN 'On Hold'
        WHEN LOWER(projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS Project_Status__c,
    CASE 
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS Go_Live_Date__c,
    kunden_kennung AS Account__c,
    opp_kennung_ref AS Opportunity__c,
    projekt_kennung AS Legacy_Project_ID__c,
    CURRENT_TIMESTAMP::text AS CreatedDate,
    CURRENT_TIMESTAMP::text AS LastModifiedDate,
    0 AS IsDeleted

FROM {{ source('fixture_master_src', 'master_projekte') }}
