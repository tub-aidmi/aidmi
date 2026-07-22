{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled') 
            THEN INITCAP(CASE 
                WHEN LOWER(TRIM(project_status__c)) = 'in planning' THEN 'In Planning'
                ELSE INITCAP(TRIM(project_status__c))
            END)
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(go_live_date__c) IS NULL OR TRIM(go_live_date__c) = '' OR go_live_date__c::TEXT = '0000-00-00' THEN NULL
        WHEN go_live_date__c::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(go_live_date__c AS DATE)::TEXT
        WHEN go_live_date__c::TEXT ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c::TEXT ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        WHEN go_live_date__c::TEXT ~ '^\d{8}$' THEN 
            CAST(
                SUBSTR(go_live_date__c, 1, 4) || '-' || 
                SUBSTR(go_live_date__c, 5, 2) || '-' || 
                SUBSTR(go_live_date__c, 7, 2) AS DATE
             )::TEXT
        ELSE NULL 
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
