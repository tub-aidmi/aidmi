{{ config(materialized='table') }}

SELECT
    installed_asset.id AS "Id",
    COALESCE(installed_asset.name, 'Unknown Asset') AS "Name",
    installed_asset.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN installed_asset.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN installed_asset.warranty_end_date__c
        ELSE NULL
    END AS "Warranty_End_Date__c",
    installed_asset.account__c AS "Account__c",
    installed_asset.project__c AS "Project__c",
    installed_asset.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS installed_asset