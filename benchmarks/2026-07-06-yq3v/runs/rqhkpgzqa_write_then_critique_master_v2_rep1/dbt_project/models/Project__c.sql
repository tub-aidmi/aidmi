{{ config(materialized='table') }}

SELECT
    MD5(projekt_kennung) AS "Id",
    COALESCE(projektname, projekt_kennung) AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum = '0000-00-00' THEN NULL
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_datum IS NOT NULL
             THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND go_live_datum IS NOT NULL
             THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{8}$' AND go_live_datum IS NOT NULL
             THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(opp_kennung_ref) AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}