{{ config(materialized='table') }}

SELECT
    -- Generate deterministic Salesforce-style Id using asset_kennung
    CONCAT('a0Y', SUBSTR(MD5(assets.asset_kennung), 1, 14)) AS "Id",

    -- Name from source, normalized (not empty after trim)
    INITCAP(TRIM(assets.asset_name)) AS "Name",

    -- Serial number (may be NULL)
    TRIM(assets.serien_nummer) AS "Serial_Number__c",

    -- Warranty end date: parse multiple formats, output ISO YYYY-MM-DD or NULL
    CASE
        WHEN assets.garantieende IS NULL THEN NULL
        WHEN assets.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN assets.garantieende::TEXT
        WHEN assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(
                TO_DATE(assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{8}$' THEN SUBSTR(assets.garantieende, 1, 4) || '-'
                                                  || SUBSTR(assets.garantieende, 5, 2) || '-'
                                                  || SUBSTR(assets.garantieende, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: reference Salesforce-style Account Id via joined customer
    CONCAT('a0X', SUBSTR(MD5(master_kunden.kundennummer), 1, 14)) AS "Account__c",

    -- Project__c: reference Salesforce-style Project Id via joined project (if exists)
    CASE WHEN master_projekte.projekt_kennung IS NOT NULL THEN
        CONCAT('a0K', SUBSTR(MD5(master_projekte.projekt_kennung), 1, 14))
    ELSE NULL END AS "Project__c",

    -- Legacy asset ID from natural key
    CAST(assets.asset_kennung AS TEXT) AS "Legacy_Asset_ID__c",

    -- Standard audit fields (no temporal data in source)
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} assets
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} master_kunden
    ON assets.kunden_kennung = master_kunden.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} master_projekte
    ON assets.projekt_kennung = master_projekte.projekt_kennung

WHERE TRIM(assets.asset_name) IS NOT NULL AND TRIM(assets.asset_name) != ''