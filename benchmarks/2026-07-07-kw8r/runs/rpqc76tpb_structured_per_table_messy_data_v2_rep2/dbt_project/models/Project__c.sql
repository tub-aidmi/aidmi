{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c IS NOT NULL AND TRIM(go_live_date__c) != '' THEN
            CASE 
                WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
                WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
                WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
WHERE id IS NOT NULL AND TRIM(id) != ''