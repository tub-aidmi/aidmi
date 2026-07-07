{{
  config(
    materialized='table'
  )
}}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name",
    CASE
        WHEN status = 'Active' THEN 'Active'
        WHEN status = 'Completed' THEN 'Completed'
        WHEN status = 'In Planning' THEN 'In Planning'
        WHEN status = 'On Hold' THEN 'On Hold'
        WHEN status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live -- Already YYYY-MM-DD
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL -- Unparseable format
    END AS "Go_Live_Date__c",
    client_id AS "Account__c", -- Assuming client_id maps to Account.Id
    opportunity_ref AS "Opportunity__c", -- Assuming opportunity_ref maps to Opportunity.Id
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }}
