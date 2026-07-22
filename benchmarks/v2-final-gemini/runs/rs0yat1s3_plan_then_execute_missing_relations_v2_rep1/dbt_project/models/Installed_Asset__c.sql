-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    TRIM(a.id) AS "Id",
    TRIM(a.name) AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    TRIM(a.warranty) AS "Warranty_End_Date__c",
    TRIM(acc.id) AS "Account__c",
    TRIM(proj.id) AS "Project__c",
    TRIM(a.id) AS "Legacy_Asset_ID__c",
    NOW()::text AS "CreatedDate",
    NOW()::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON (TRIM(a.client) = TRIM(acc.id) OR TRIM(a.client) = TRIM(acc.name))
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON (TRIM(a.project) = TRIM(proj.id) OR TRIM(a.project) = TRIM(proj.name))