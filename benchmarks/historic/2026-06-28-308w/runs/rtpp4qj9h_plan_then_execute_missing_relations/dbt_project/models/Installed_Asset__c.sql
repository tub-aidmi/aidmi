
{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unnamed Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(asset.warranty AS DATE), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset.client AS "Account__c",
    asset.project AS "Project__c",
    NULL AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Asset') }} AS asset
