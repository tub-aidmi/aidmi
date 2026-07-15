{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    name AS "Name",
    CASE status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND TRIM(go_live) != ''
            THEN CASE
                WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live, 'YYYY-MM-DD')::TEXT
                WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
                WHEN go_live ~ '^\d{8}$' THEN TO_DATE(go_live, 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account__c: transform CUST-XXXX to Salesforce Account.Id ('001' prefix, 12-digit padding)
    '001' || LPAD(SUBSTRING(kd FROM '\d+')::INTEGER::TEXT, 12, '0') AS "Account__c",
    -- Opportunity__c: transform OPP-XXXXX to Salesforce Opportunity.Id ('006' prefix, 12-digit padding)
    '006' || LPAD(SUBSTRING(opp FROM '\d+')::INTEGER::TEXT, 12, '0') AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}