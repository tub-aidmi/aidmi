{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    
    COALESCE(TRIM(name), 'Unknown') AS "Name",

    CASE 
        WHEN UPPER(TRIM(COALESCE(status, ''))) IN ('ACTIVE', 'COMPLETED', 'IN PLANNING', 'ON HOLD', 'CANCELLED')
            THEN INITCAP(LOWER(TRIM(COALESCE(status, ''))))
            ELSE NULL::text
    END AS "Project_Status__c",

    CASE 
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$'     THEN TO_DATE(go_live, 'YYYY-MM-DD')::TEXT
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{2}\.\d{2}\.\d{4}$'   THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
        ELSE NULL::text
    END AS "Go_Live_Date__c",

    client_id AS "Account__c",
    
    opportunity_ref AS "Opportunity__c",

    id AS "Legacy_Project_ID__c",

    NULL::text  AS "CreatedDate",
    NULL::text  AS "LastModifiedDate",
    0           AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }}