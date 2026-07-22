{{ config(materialized='table') }}

WITH master_assets AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
),
master_kunden AS (
    SELECT
        kundennummer,
        unternehmensname
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
master_projekte AS (
    SELECT
        projekt_kennung,
        projektname
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    MD5(asset.asset_kennung)::TEXT AS "Id",
    COALESCE(TRIM(asset.asset_name), 'Unknown Asset Name') AS "Name",
    asset.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.garantieende::DATE::TEXT -- YYYY-MM-DD
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(asset.garantieende, 'DD.MM.YYYY')::TEXT -- DD.MM.YYYY
        WHEN asset.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(asset.garantieende, 'MM/DD/YYYY')::TEXT -- MM/DD/YYYY
        WHEN asset.garantieende ~ '^\d{8}$' THEN TO_DATE(asset.garantieende, 'YYYYMMDD')::TEXT -- YYYYMMDD
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(customer.kundennummer)::TEXT AS "Account__c",
    MD5(project.projekt_kennung)::TEXT AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    master_assets AS asset
LEFT JOIN
    master_kunden AS customer
ON
    asset.kunden_kennung = customer.kundennummer
LEFT JOIN
    master_projekte AS project
ON
    asset.projekt_kennung = project.projekt_kennung