{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)
SELECT
    MD5(asset_kennung) AS "Id",
    COALESCE(asset_name, 'Unknown Asset') AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE
        WHEN garantieende IS NULL THEN NULL
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL -- Nullable target, so NULL if unparseable
    END AS "Warranty_End_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(projekt_kennung) AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data
