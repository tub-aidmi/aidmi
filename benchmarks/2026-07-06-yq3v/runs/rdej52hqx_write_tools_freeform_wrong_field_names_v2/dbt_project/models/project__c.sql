{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    p.name AS "Name",
    CASE
        WHEN p.status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN p.status
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CAST(p.go_live AS DATE), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    p.kd AS "Account__c", -- This is kunden_nr from kunden, which is the Account.Id
    p.opp AS "Opportunity__c", -- This is chance_id from chancen, which is the Opportunity.Id
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
