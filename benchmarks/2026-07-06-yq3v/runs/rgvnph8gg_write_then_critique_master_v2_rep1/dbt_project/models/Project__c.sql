-- depends_on: {{ ref('Account') }} -- depends_on: {{ ref('Opportunity') }}
{{ config(materialized='table') }}

SELECT
    TRIM(mp.projekt_kennung) AS "Id",
    COALESCE(mp.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(mp.kunden_kennung) AS "Account__c",
    MD5(mp.opp_kennung_ref) AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp