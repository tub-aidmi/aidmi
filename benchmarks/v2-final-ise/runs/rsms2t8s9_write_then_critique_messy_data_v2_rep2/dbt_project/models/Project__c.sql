{{ config(materialized='table') }}

SELECT 
    COALESCE(id, 'Unknown') AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN to_char(to_date(go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_char(to_date(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_date(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}