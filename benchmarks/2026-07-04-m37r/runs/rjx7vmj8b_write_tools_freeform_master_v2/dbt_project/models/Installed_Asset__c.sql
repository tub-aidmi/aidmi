-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    MD5(asset_kennung) AS "Id",
    COALESCE(TRIM(asset_name), TRIM(asset_kennung)) AS "Name", -- Name is NOT NULL
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN garantieende IN ('N/A', '0000-00-00') THEN NULL -- Handle 'N/A' and '0000-00-00' as NULL
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende::DATE -- YYYY-MM-DD
        WHEN garantieende ~ '^\d{8}$' THEN TO_DATE(garantieende, 'YYYYMMDD') -- YYYYMMDD (Not seen in sample, but for consistency if it appears)
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantieende, 'DD.MM.YYYY') -- DD.MM.YYYY
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(garantieende, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c", -- Target is TEXT, output as ISO YYYY-MM-DD
    MD5(kunden_kennung) AS "Account__c", -- Use consistent Account Id generation
    MD5(projekt_kennung) AS "Project__c", -- Use consistent Project Id generation
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }}
