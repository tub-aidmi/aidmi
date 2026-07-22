{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.warranty::DATE
            WHEN asset.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(asset.warranty, 'DD.MM.YYYY')
            WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(asset.warranty, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    asset.client AS "Account__c",
    asset.project AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
