{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'N/A') AS "Name",
    CASE UPPER(TRIM(status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' THEN go_live -- YYYY-MM-DD
        WHEN go_live ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{{8}}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c", -- Assuming client_id is the Salesforce Account Id
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
