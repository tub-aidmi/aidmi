{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'N/A') AS "Name",
    CASE
        WHEN UPPER(TRIM(project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        COALESCE(
            TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD'),
            TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'),
            TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'),
            TO_DATE(TRM(go_live_date__c), 'YYYYMMDD')
        ),
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
