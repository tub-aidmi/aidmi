{{ config(materialized='table') }}

SELECT
    MD5(asset.asset_kennung) AS "Id",
    COALESCE(asset.asset_name, asset.asset_kennung) AS "Name",
    asset.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(kunden.kundennummer) AS "Account__c",
    MD5(projekt.projekt_kennung) AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON asset.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt
    ON asset.projekt_kennung = projekt.projekt_kennung
