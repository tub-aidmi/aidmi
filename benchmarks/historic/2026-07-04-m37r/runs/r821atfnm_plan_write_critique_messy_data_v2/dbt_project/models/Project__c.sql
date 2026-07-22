-- depends_on: {{ source('fixture_messy_data_v2_src', 'project__c') }}

{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Project') AS "Name",
    CASE
        WHEN TRIM(INITCAP(project_status__c)) = 'Active' THEN 'Active'
        WHEN TRIM(INITCAP(project_status__c)) = 'Completed' THEN 'Completed'
        WHEN TRIM(INITCAP(project_status__c)) = 'In Planning' THEN 'In Planning'
        WHEN TRIM(INITCAP(project_status__c)) = 'On Hold' THEN 'On Hold'
        WHEN TRIM(INITCAP(project_status__c)) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN go_live_date__c = '0000-00-00' THEN NULL
            WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_date__c, 'YYYY-MM-DD')
            WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')
            WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')
            WHEN go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}