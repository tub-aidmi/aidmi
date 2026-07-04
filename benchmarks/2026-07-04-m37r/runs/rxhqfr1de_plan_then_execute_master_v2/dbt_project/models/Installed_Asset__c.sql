{{ config(materialized='table') }}

SELECT
    -- Id: Generate deterministic 18-character ID
    SUBSTRING(MD5(ma.asset_kennung), 1, 18) AS "Id",

    -- Name: Asset name, with default if NULL, trimmed and title-cased
    INITCAP(TRIM(COALESCE(ma.asset_name, 'Unnamed Asset'))) AS "Name",

    -- Serial_Number__c: Trimmed serial number
    TRIM(ma.serien_nummer) AS "Serial_Number__c",

    -- Warranty_End_Date__c: Parsed and formatted warranty end date
    CASE
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: Deterministic Account ID derived from customer number
    SUBSTRING(MD5(mk.kundennummer), 1, 18) AS "Account__c",

    -- Project__c: Deterministic Project ID derived from project identifier
    SUBSTRING(MD5(mp.projekt_kennung), 1, 18) AS "Project__c",

    -- Legacy_Asset_ID__c: Original asset identifier from the source system
    ma.asset_kennung AS "Legacy_Asset_ID__c",

    -- CreatedDate: No source mapping, set to NULL
    NULL AS "CreatedDate",

    -- LastModifiedDate: No source mapping, set to NULL
    NULL AS "LastModifiedDate",

    -- IsDeleted: No source mapping, default to 0
    0 AS "IsDeleted"

FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON ma.projekt_kennung = mp.projekt_kennung
