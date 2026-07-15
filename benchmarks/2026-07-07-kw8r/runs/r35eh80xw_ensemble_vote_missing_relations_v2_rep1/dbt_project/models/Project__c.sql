{{ config(materialized='table') }}

SELECT 
    p.id AS "Id",
    COALESCE(TRIM(p.name), '') AS "Name",
    CASE LOWER(TRIM(COALESCE(p.status, '')))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'canceled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    CASE 
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != '' THEN
            CASE
                -- DD.MM.YYYY format (European common)
                WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
                -- YYYY-MM-DD format (ISO)
                WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN LEFT(TRIM(p.go_live), 10)
                -- MM/DD/YYYY format (US)
                WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
                -- YYYYMMDD compact
                WHEN LENGTH(TRIM(p.go_live)) = 8 AND TRIM(p.go_live) ~ '^\d{8}$' 
                    THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYYMMDD'), 'YYYY-MM-DD')
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    '' AS "CreatedDate",
    '' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(p.client_id) = TRIM(a.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(p.opportunity_ref) = TRIM(o.id)