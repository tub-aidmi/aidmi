-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, a.id) AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc_by_id.id, acc_by_name.id) AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_by_id
    ON a.client = acc_by_id.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_by_name
    ON a.client = acc_by_name.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON a.project = p.id