{{ config(materialized='table') }}

SELECT
    CAST(proj_id AS TEXT) AS "Id",
    coalesce(trim(name), 'Unknown') AS "Name",
    case
        when upper(trim(status)) = 'ACTIVE' then 'Active'
        when upper(trim(status)) = 'COMPLETED' then 'Completed'
        when upper(trim(status)) = 'IN PLANNING' then 'In Planning'
        when upper(trim(status)) = 'ON HOLD' then 'On Hold'
        when upper(trim(status)) = 'CANCELLED' then 'Cancelled'
        else 'In Planning'
    end as "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
