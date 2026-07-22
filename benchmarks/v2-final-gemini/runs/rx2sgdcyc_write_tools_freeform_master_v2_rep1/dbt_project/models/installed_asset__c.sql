{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_kennung) AS "Id",
    assets.asset_name AS "Name",
    assets.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(kunden.kundennummer) AS "Account__c",
    MD5(projekte.projekt_kennung) AS "Project__c",
    assets.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS assets
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    assets.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
ON
    assets.projekt_kennung = projekte.projekt_kennung
