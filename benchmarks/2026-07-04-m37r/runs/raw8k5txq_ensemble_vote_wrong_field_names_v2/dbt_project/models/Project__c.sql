{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, 'Unknown Project') AS "Name",
    CASE
        WHEN proj.status = 'Active' THEN 'Active'
        WHEN proj.status = 'Completed' THEN 'Completed'
        WHEN proj.status = 'In Planning' THEN 'In Planning'
        WHEN proj.status = 'On Hold' THEN 'On Hold'
        WHEN proj.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(proj.go_live AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden.kunden_nr AS "Account__c", -- This will be the Legacy_Customer_ID__c, which maps to Salesforce Account Id in a later stage
    chancen.chance_id AS "Opportunity__c", -- This will be the Legacy_Opportunity_ID__c, which maps to Salesforce Opportunity Id in a later stage
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON proj.kd = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON proj.opp = chancen.chance_id
