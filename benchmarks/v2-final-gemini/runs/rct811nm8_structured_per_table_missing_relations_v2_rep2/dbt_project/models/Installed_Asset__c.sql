-- depends_on: {{ ref('account') }}
-- depends_on: {{ ref('project__c') }}

{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown Asset') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
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
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src_acc
    ON a.client = src_acc.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS src_proj
    ON a.project = src_proj.id
LEFT JOIN
    account AS acc
    ON src_acc.id = acc.Id
LEFT JOIN
    project__c AS p
    ON src_proj.id = p.Id