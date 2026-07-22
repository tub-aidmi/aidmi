{{ config(materialized='table') }}

SELECT
    TRIM(asset.id) AS "Id",
    COALESCE(TRIM(asset.name), 'Unnamed Asset ' || TRIM(asset.id)) AS "Name",
    TRIM(asset.serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN TRIM(asset.warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty_end_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(asset.warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty_end_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(asset.warranty_end_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(asset.warranty_end_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(asset.account__c) AS "Account__c",
    TRIM(asset.project__c) AS "Project__c",
    TRIM(asset.id) AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS asset
WHERE
    asset.id IS NOT NULL
