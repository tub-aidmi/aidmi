-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    p.name AS "Name",
    p.status AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON p.kd = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
    ON p.opp = c.chance_id