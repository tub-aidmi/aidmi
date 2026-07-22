{{ config(materialized='table') }}

SELECT
    MD5(UPPER(TRIM(mp.projekt_kennung))) AS "Id",
    COALESCE(TRIM(mp.projektname), TRIM(mp.projekt_kennung)) AS "Name",
    CASE LOWER(TRIM(mp.projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(UPPER(TRIM(mp.kunden_kennung))) AS "Account__c",
    MD5(UPPER(TRIM(mp.opp_kennung_ref))) AS "Opportunity__c",
    TRIM(mp.projekt_kennung) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
WHERE
    mp.projekt_kennung IS NOT NULL
