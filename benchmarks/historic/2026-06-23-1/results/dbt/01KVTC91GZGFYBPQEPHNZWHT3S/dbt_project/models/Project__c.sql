{{ config(materialized='table') }}
SELECT
    mp.projekt_kennung AS Id,
    mp.projektname AS Name,
    CASE
        WHEN mp.projektstatus IN ('In Bearbeitung', NULL) THEN 'In Planning'
        WHEN mp.projektstatus = 'Active' THEN 'Active'
        WHEN mp.projektstatus = 'Inactive' THEN 'On Hold'
        WHEN mp.projektstatus = 'Completed' THEN 'Completed'
        WHEN mp.projektstatus = 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Fallback for unanticipated values
    END AS Project_Status__c,
    CASE
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN
            mp.go_live_datum
        WHEN mp.go_live_datum IS NULL THEN
            NULL
        ELSE
            NULL -- Fallback for unparsable dates
    END AS Go_Live_Date__c,
    mp.kunden_kennung AS Account__c,
    mp.opp_kennung_ref AS Opportunity__c,
    mp.projekt_kennung AS Legacy_Project_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_projekte') }} mp