{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_kennung) AS "Id",
    COALESCE(TRIM(assets.asset_name), 'Unknown Asset') AS "Name",
    TRIM(assets.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(assets.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(assets.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(assets.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(assets.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(assets.garantieende) ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(assets.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(kunden.kundennummer) AS "Account__c",
    MD5(projekte.projekt_kennung) AS "Project__c",
    assets.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS assets
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON assets.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
    ON assets.projekt_kennung = projekte.projekt_kennung
