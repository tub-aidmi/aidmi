{{ config(materialized='table') }}
SELECT 
    "asset_kennung" AS "Id",
    TRIM("asset_name") AS "Name",
    TRIM("serien_nummer") AS "Serial_Number__c",
    CASE 
        WHEN TRIM("garantieende") ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("garantieende", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("garantieende") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("garantieende", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("garantieende") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("garantieende", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("garantieende") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM("garantieende")
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    COALESCE(
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = TRIM("kunden_kennung") LIMIT 1),
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = "kunden_kennung" LIMIT 1)
    ) AS "Account__c",
    COALESCE(
        (SELECT "projekt_kennung" FROM {{ source('fixture_master_v2_src', 'master_projekte') }} WHERE "projekt_kennung" = TRIM("projekt_kennung") LIMIT 1),
        (SELECT "projekt_kennung" FROM {{ source('fixture_master_v2_src', 'master_projekte') }} WHERE "projekt_kennung" = "projekt_kennung" LIMIT 1)
    ) AS "Project__c",
    "asset_kennung" AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}