{{ config(materialized='table') }}

WITH account_lookup AS (
    SELECT
        INITCAP(TRIM(kundennummer)) AS "Id",
        kundennummer                AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

project_lookup AS (
    SELECT
        INITCAP(TRIM(projekt_kennung)) AS "Id"
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

raw_assets AS (
    SELECT
        asset_kennung                AS raw_legacy_asset_id,
        asset_name                   AS raw_asset_name,
        serien_nummer                AS "Serial_Number__c",
        INITCAP(TRIM(asset_kennung)) AS "Id",
        INITCAP(TRIM(kunden_kennung)) AS account_raw_key,
        INITCAP(TRIM(projekt_kennung)) AS project_raw_key,
        garantieende                 AS raw_warranty_end
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)

SELECT
    r."Id",
    CASE
        WHEN TRIM(r.raw_asset_name) = '' OR r.raw_asset_name IS NULL THEN 'Unknown Asset'
        ELSE INITCAP(TRIM(r.raw_asset_name))
    END AS "Name",
    r."Serial_Number__c",

    CASE
        WHEN r.raw_warranty_end IS NOT NULL AND TRIM(r.raw_warranty_end) != ''
        THEN
            CASE
                WHEN TRIM(r.raw_warranty_end) ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_DATE(TRIM(r.raw_warranty_end), 'DD.MM.YYYY')::TEXT
                WHEN TRIM(r.raw_warranty_end) ~ '^\d{8}$'
                    THEN TO_DATE(TRIM(r.raw_warranty_end), 'YYYYMMDD')::TEXT
                WHEN TRIM(r.raw_warranty_end) ~ '^\d{2}/\d{2}/\d{4}$'
                    THEN TO_DATE(TRIM(r.raw_warranty_end), 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",

    al."Id"  AS "Account__c",
    pl."Id"  AS "Project__c",
    r.raw_legacy_asset_id AS "Legacy_Asset_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM raw_assets r
LEFT JOIN account_lookup al
    ON r.account_raw_key = al."Id"
LEFT JOIN project_lookup pl
    ON r.project_raw_key = pl."Id"