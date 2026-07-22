{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(status) = 'active' THEN 'Active'
        WHEN LOWER(status) = 'completed' THEN 'Completed'
        WHEN LOWER(status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    -- Date parsing: assuming YYYY-MM-DD, DD.MM.YYYY, YYYYMMDD, MM/DD/YYYY formats
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kd AS "Account__c", -- Maps to Account.Id (kunden_nr)
    opp AS "Opportunity__c", -- Maps to Opportunity.Id (chance_id)
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
