{{
    config(materialized='table')
}}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc_by_id.id, acc_by_name.id) AS "Account__c",
    proj.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_by_id
    ON asset.client = acc_by_id.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_by_name
    ON asset.client = acc_by_name.name
    AND acc_by_id.id IS NULL
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON asset.project = proj.id