{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown Asset') AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON a.client = acc.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON a.project = p.id
