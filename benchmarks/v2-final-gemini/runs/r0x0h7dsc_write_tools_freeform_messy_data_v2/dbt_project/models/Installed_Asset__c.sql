{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unnamed Asset') AS "Name",
    asset.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN asset.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(asset.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN asset.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN asset.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset.account__c AS "Account__c",
    asset.project__c AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c", -- Source natural key
    '2023-01-01' AS "CreatedDate", -- Default value
    '2023-01-01' AS "LastModifiedDate", -- Default value
    0 AS "IsDeleted" -- Default value
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS asset
