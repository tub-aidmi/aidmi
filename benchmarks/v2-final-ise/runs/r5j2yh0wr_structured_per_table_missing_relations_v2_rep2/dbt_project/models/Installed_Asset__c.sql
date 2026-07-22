{{ config(materialized='table') }}

SELECT 
    asset.id AS "Id",
    COALESCE(NULLIF(TRIM(asset.name), ''), 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE 
        WHEN asset.warranty IS NULL OR TRIM(asset.warranty) = '' THEN NULL
        WHEN asset.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(asset.warranty)
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    account.id AS "Account__c",
    asset.project AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} asset
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} account 
    ON TRIM(asset.client) = TRIM(account.id)