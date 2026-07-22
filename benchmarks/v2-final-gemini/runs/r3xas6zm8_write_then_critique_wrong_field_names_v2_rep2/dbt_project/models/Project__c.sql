{{ config(materialized='table') }}

SELECT
    TRIM(proj.proj_id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project ' || TRIM(proj.proj_id)) AS "Name",
    CASE UPPER(TRIM(proj.status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'Active' -- Default to Active for unmapped statuses, as per reviewer comment
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(proj.kd) AS "Account__c",
    TRIM(proj.opp) AS "Opportunity__c",
    TRIM(proj.proj_id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj