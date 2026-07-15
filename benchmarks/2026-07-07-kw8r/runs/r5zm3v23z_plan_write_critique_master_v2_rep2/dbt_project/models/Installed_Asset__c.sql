WITH asset_base AS (
    SELECT
        a.asset_kennung,
        a.asset_name,
        a.serien_nummer,
        a.garantieende,
        a.kunden_kennung,
        a.projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
),

-- Join customers for Account__c lookup
assets_with_account AS (
    SELECT
        ab.*,
        k.kundennummer AS matched_kundennummer
    FROM asset_base ab
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
        ON TRIM(ab.kunden_kennung) = TRIM(k.kundennummer)
),

-- Join projects for Project__c lookup
assets_full AS (
    SELECT
        aa.*,
        p.projekt_kennung AS matched_projekt_kennung
    FROM assets_with_account aa
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
        ON TRIM(aa.projekt_kennung) = TRIM(p.projekt_kennung)
),

parsed_dates AS (
    SELECT
        *,
        -- Warranty date: DD.MM.YYYY, YYYY-MM-DD, or YYYYMMDD patterns; NULL on unparseable
        CASE
            WHEN garantieende IS NULL THEN NULL
            WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantieende, 'DD.MM.YYYY')::TEXT
            WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende
            WHEN garantieende ~ '^\d{8}$' THEN TO_DATE(garantieende, 'YYYYMMDD')::TEXT
            ELSE NULL
        END AS parsed_warranty_date
    FROM assets_full
)

SELECT
    -- Id: Salesforce-style ID generated from asset_kennung natural key
    'a1Y' || MD5(asset_kennung) AS "Id",

    -- Name: INITCAP of trimmed asset_name; fallback for empty/null
    COALESCE(INITCAP(TRIM(asset_name)), 'Unnamed Asset') AS "Name",

    -- Serial_Number__c: trimmed, NULL allowed
    TRIM(serien_nummer) AS "Serial_Number__c",

    -- Warranty_End_Date__c: parsed date or NULL
    parsed_warranty_date AS "Warranty_End_Date__c",

    -- Account__c: Salesforce Account ID via matched kundennummer → 'A' || MD5
    CASE
        WHEN matched_kundennummer IS NOT NULL THEN 'A' || MD5(matched_kundennummer)
        ELSE NULL
    END AS "Account__c",

    -- Project__c: Salesforce Project ID via matched projekt_kennung → 'a2X' || MD5
    CASE
        WHEN matched_projekt_kennung IS NOT NULL THEN 'a2X' || MD5(matched_projekt_kennung)
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: natural key preserved
    asset_kennung AS "Legacy_Asset_ID__c",

    -- CreatedDate / LastModifiedDate: not in source, set NULL
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",

    -- IsDeleted: default 0 (false)
    0 AS "IsDeleted"

FROM parsed_dates