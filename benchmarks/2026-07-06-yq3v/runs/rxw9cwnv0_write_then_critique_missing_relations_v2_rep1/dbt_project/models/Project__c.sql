-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile
{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, '') AS "Name",
    CASE
        WHEN p.status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN p.status
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live IS NOT NULL THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    acc.id AS "Account__c",
    opp.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    p.client_id = acc.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
ON
    p.opportunity_ref = opp.id