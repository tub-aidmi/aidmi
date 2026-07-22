{{ config(materialized='table') }}

SELECT
    TRIM(asset.id) AS "Id",
    COALESCE(TRIM(asset.name), 'Unknown Asset') AS "Name",
    TRIM(asset.serial) AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(asset.warranty), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(asset.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(asset.warranty), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    TRIM(asset.client) AS "Account__c",
    TRIM(asset.project) AS "Project__c",
    TRIM(asset.id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
