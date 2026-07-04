-- depends_on: {{ ref('Account') }} {{ ref('Opportunity') }}

{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    p.name AS "Name",
    CASE p.status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    p.kd AS "Account__c", -- Maps to Account.Id (which is kunden.kunden_nr)
    p.opp AS "Opportunity__c", -- Maps to Opportunity.Id (which is chancen.chance_id)
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p