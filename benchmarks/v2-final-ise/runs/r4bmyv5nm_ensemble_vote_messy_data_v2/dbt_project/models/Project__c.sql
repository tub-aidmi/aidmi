{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    name AS "Name",
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
        -- YYYYMMDD (8 digits only)
        WHEN go_live_date__c ~ '^\d{8}$' THEN 
            CASE 
                WHEN SUBSTRING(go_live_date__c, 5, 2)::INT BETWEEN 1 AND 12 
                     AND SUBSTRING(go_live_date__c, 7, 2)::INT BETWEEN 1 AND 31
                THEN SUBSTRING(go_live_date__c, 1, 4) || '-' || 
                     SUBSTRING(go_live_date__c, 5, 2) || '-' || 
                     SUBSTRING(go_live_date__c, 7, 2)
                ELSE NULL
            END
        -- DD.MM.YYYY (European dot-separated)
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        -- MM/DD/YYYY (US slash-separated)
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        -- Already ISO YYYY-MM-DD or similar; pass through if pattern matches
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}' THEN TRIM(go_live_date__c)
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}