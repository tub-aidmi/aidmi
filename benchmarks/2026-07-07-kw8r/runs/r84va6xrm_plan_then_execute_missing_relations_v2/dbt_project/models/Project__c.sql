{{ config(materialized='table') }}

WITH normalized_project AS (
    SELECT 
        *
        ,TRIM(UPPER(REGEXP_REPLACE(client_id, '[^A-Z0-9]', '', 'g'))) AS norm_client_id
        ,TRIM(UPPER(REGEXP_REPLACE(opportunity_ref, '[^A-Z0-9]', '', 'g'))) AS norm_opportunity_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
),

normalized_accounts AS (
    SELECT 
        id
        ,TRIM(UPPER(REGEXP_REPLACE(id, '[^A-Z0-9]', '', 'g'))) AS norm_id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),

normalized_opportunities AS (
    SELECT 
        id
        ,TRIM(UPPER(REGEXP_REPLACE(id, '[^A-Z0-9]', '', 'g'))) AS norm_id
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
)

SELECT 
    TRIM(np.id) AS "Id"
    ,COALESCE(TRIM(INITCAP(np.name)), 'Untitled Project') AS "Name"
    ,CASE LOWER(TRIM(np.status))
        WHEN 'active'      THEN 'Active'
        WHEN 'completed'   THEN 'Completed'
        WHEN 'done'        THEN 'Completed'
        WHEN 'closed'      THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'planning'    THEN 'In Planning'
        WHEN 'on hold'     THEN 'On Hold'
        WHEN 'paused'      THEN 'On Hold'
        WHEN 'stopped'     THEN 'On Hold'
        WHEN 'cancelled'   THEN 'Cancelled'
        WHEN 'canceled'    THEN 'Cancelled'
        WHEN 'void'        THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c"
    ,CASE 
        WHEN np.go_live IS NOT NULL AND TRIM(np.go_live) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN SUBSTRING(TRIM(np.go_live) FROM 1 FOR 10)
        WHEN np.go_live IS NOT NULL AND TRIM(np.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(np.go_live), 'DD.MM.YYYY')::TEXT
        WHEN np.go_live IS NOT NULL AND TRIM(np.go_live) ~ '^\d{8}$'
            THEN SUBSTRING(TRIM(np.go_live) FROM 1 FOR 4) || '-'
               || SUBSTRING(TRIM(np.go_live) FROM 5 FOR 2) || '-'
               || SUBSTRING(TRIM(np.go_live) FROM 7 FOR 2)
        ELSE NULL
    END AS "Go_Live_Date__c"
    ,na.norm_id  AS "Account__c"
    ,no.norm_id  AS "Opportunity__c"
    ,TRIM(np.id) AS "Legacy_Project_ID__c"
    ,CURRENT_DATE::TEXT AS "CreatedDate"
    ,CURRENT_DATE::TEXT AS "LastModifiedDate"
    ,0            AS "IsDeleted"

FROM normalized_project np
LEFT JOIN normalized_accounts      na ON np.norm_client_id  = na.norm_id
LEFT JOIN normalized_opportunities no ON np.norm_opportunity_ref = no.norm_id