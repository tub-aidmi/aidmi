{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE
        WHEN UPPER(status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(status) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(status) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(status) = 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning' -- Default value for Project_Status__c, or NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c", -- References Account.Id
    opportunity_ref AS "Opportunity__c", -- References Opportunity.Id
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }}
