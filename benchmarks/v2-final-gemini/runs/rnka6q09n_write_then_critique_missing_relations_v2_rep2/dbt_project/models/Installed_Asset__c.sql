{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown Asset') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON a.client = acc.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON a.project = p.id