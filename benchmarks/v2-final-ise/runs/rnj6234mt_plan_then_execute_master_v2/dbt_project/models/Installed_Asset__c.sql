{{ config(materialized='table') }}

SELECT 
    'ASSET-' || asset_kennung AS "Id",
    COALESCE(INITCAP(TRIM(asset_name)), 'Unnamed Asset') AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    'CUST-' || kunden_kennung AS "Account__c",
    'PROJ-' || projekt_kennung AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}