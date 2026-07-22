-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_kennung) AS "Id",
    COALESCE(assets.asset_name, assets.asset_kennung) AS "Name", -- Name is NOT NULL, fallback to asset_kennung
    assets.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN assets.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(assets.garantieende, 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(assets.garantieende, 'DD.MM.YYYY')
        WHEN assets.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(assets.garantieende, 'MM/DD/YYYY')
        WHEN assets.garantieende ~ '^\d{8}$' THEN TO_DATE(assets.garantieende, 'YYYYMMDD')
        ELSE NULL -- Can be NULL, so NULL if unparseable
    END AS "Warranty_End_Date__c",
    MD5(assets.kunden_kennung) AS "Account__c",
    MD5(assets.projekt_kennung) AS "Project__c",
    assets.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS assets