{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'Unknown Project') AS "Name",
    CASE UPPER(TRIM(project_status__c))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ) AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
