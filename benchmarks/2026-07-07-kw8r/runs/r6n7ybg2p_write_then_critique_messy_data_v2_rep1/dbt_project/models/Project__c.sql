{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    CASE 
        WHEN TRIM(COALESCE(p.name, '')) = '' THEN 'Unknown'
        ELSE INITCAP(TRIM(p.name))
    END AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_date__c IS NOT NULL AND TRIM(p.go_live_date__c) != '' THEN
            CASE 
                WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'DD.MM.YYYY')::TEXT
                WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYY-MM-DD')::TEXT
                WHEN p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'MM/DD/YYYY')::TEXT
                WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON TRIM(p.account__c) = TRIM(a.erp_number__c) OR TRIM(p.account__c) = TRIM(a.legacy_customer_id__c)
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
    ON TRIM(p.opportunity__c) = TRIM(o.id)