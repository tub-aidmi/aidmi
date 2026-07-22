{{ config(materialized='table') }}

SELECT
    asset_src.id AS "Id",
    COALESCE(TRIM(asset_src.name), 'Unknown Asset') AS "Name",
    TRIM(asset_src.serial) AS "Serial_Number__c",
    CASE
        WHEN asset_src.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset_src.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_src.id AS "Account__c",
    project_src.id AS "Project__c",
    asset_src.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset_src
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_src
    ON asset_src.client = account_src.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project_src
    ON asset_src.project = project_src.id