{{ config(materialized='table') }}

SELECT
    -- Consistent 18-char uppercase hash-based Id for this asset
    UPPER(SUBSTR(MD5(TRIM(a.asset_kennung)), 1, 18)) AS "Id",

    -- Name is NOT NULL in target; COALESCE supplies default when source is missing
    COALESCE(a.asset_name, 'Unknown Asset') AS "Name",
    a.serien_nummer AS "Serial_Number__c",

    -- Parse warranty end date from DD.MM.YYYY format; NULL if missing or unparseable
    CASE
        WHEN a.garantieende IS NULL OR TRIM(a.garantieende) = '' THEN NULL
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account foreign key: MUST match Account model's Id formula exactly (001 + 15-char MD5)
    CASE WHEN k.kundennummer IS NOT NULL THEN CONCAT('001', SUBSTRING(MD5(TRIM(k.kundennummer)), 1, 15)) ELSE NULL END AS "Account__c",

    -- Project foreign key: MUST match Project__c model's Id formula exactly (00P + 14-char lowercase MD5)
    CASE WHEN pa.projekt_kennung IS NOT NULL THEN '00P' || LEFT(LOWER(MD5(TRIM(pa.projekt_kennung))), 14) ELSE NULL END AS "Project__c",

    -- Legacy natural key for row-level verification
    a.asset_kennung AS "Legacy_Asset_ID__c",

    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a

LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON a.kunden_kennung = k.kundennummer

LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} pa
    ON a.projekt_kennung = pa.projekt_kennung