{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project Name') AS "Name",
    p.status AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON p.client_id = a.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
    ON p.opportunity_ref = o.id
