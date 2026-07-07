-- dbt model for Project__c

{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    name AS "Name",
    CASE
        WHEN status = 'Active' THEN 'Active'
        WHEN status = 'Completed' THEN 'Completed'
        WHEN status = 'In Planning' THEN 'In Planning'
        WHEN status = 'On Hold' THEN 'On Hold'
        WHEN status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CAST(go_live AS DATE), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    kd AS "Account__c", -- Assuming 'kd' is the kunden_nr from the Account table
    opp AS "Opportunity__c", -- Assuming 'opp' is the chance_id from the Opportunity table
    proj_id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
