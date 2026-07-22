{{ config(materialized='table') }}

SELECT
    MD5(TRIM(assets.asset_kennung)) AS "Id",
    TRIM(assets.asset_name) AS "Name",
    TRIM(assets.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(TRIM(assets.kunden_kennung)) AS "Account__c",
    MD5(TRIM(assets.projekt_kennung)) AS "Project__c",
    TRIM(assets.asset_kennung) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS assets
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON TRIM(assets.kunden_kennung) = TRIM(kunden.kundennummer)
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
    ON TRIM(assets.projekt_kennung) = TRIM(projekte.projekt_kennung)
