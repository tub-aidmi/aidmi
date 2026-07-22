{{ config(materialized='table') }}

WITH normalized AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung,
        REGEXP_REPLACE(TRIM(UPPER(asset_kennung)), '^[A-Z]+', '', 'g') AS canonical_asset_id,
        REGEXP_REPLACE(TRIM(UPPER(kunden_kennung)), '^[A-Z]+', '', 'g') AS canonical_account_id,
        REGEXP_REPLACE(TRIM(UPPER(projekt_kennung)), '^[A-Z]+', '', 'g') AS canonical_project_id
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)

SELECT
    canonical_asset_id AS "Id",
    COALESCE(TRIM(INITCAP(asset_name)), 'Unknown Asset') AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(garantieende) IS NOT NULL AND TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantieende) IS NOT NULL AND TRIM(garantieende) ~ '^\d{8}$' THEN TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT
        WHEN TRIM(garantieende) IS NOT NULL AND TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(garantieende)
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    canonical_account_id AS "Account__c",
    canonical_project_id AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    '2024-01-01T00:00:00.000+0000' AS "CreatedDate",
    '2024-01-01T00:00:00.000+0000' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM normalized