
{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, asset.id) AS "Name",
    asset.serial AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN asset.warranty ~ '^''\d{4}-\d{2}-\d{2}''$' THEN CAST(asset.warranty AS DATE)
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    asset.client AS "Account__c",
    asset.project AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Asset') }} AS asset
