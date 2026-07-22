-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(mp.projektname, 'Untitled Project ' || mp.projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(mp.projektstatus)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(mp.projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    mp.kunden_kennung AS "Account__c", -- Assuming kunden_kennung can serve as a Salesforce Account Id for initial load
    mp.opp_kennung_ref AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp