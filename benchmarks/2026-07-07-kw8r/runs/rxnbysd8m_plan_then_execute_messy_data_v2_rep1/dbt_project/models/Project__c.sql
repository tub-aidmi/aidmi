{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(INITCAP(TRIM(src.name)), 'Unknown') AS "Name",
    CASE
        WHEN INITCAP(TRIM(src.project_status__c)) IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled')
        THEN INITCAP(TRIM(src.project_status__c))
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src.go_live_date__c ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src.account__c AS "Account__c",
    src.opportunity__c AS "Opportunity__c",
    src.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS src