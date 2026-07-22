-- depends_on: {{ ref('account') }}
-- depends_on: {{ ref('opportunity') }}

{{ config(materialized='table') }}

SELECT
    source.id AS "Id",
    COALESCE(TRIM(INITCAP(source.name)), 'Unknown') AS "Name",
    CASE
        WHEN TRIM(UPPER(source.project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN TRIM(UPPER(source.project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN TRIM(UPPER(source.project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN TRIM(UPPER(source.project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN TRIM(UPPER(source.project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN source.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND IS_DATE(source.go_live_date__c, 'YYYY-MM-DD') THEN source.go_live_date__c
        WHEN source.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(source.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN source.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(source.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN source.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(source.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    source.account__c AS "Account__c",
    source.opportunity__c AS "Opportunity__c",
    source.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS source