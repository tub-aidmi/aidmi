{{ config(materialized='table') }}

SELECT
    asset_kennung AS "Id",
    TRIM(asset_name) AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende -- YYYY-MM-DD
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
        WHEN garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(projekt_kennung) AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_assets') }}
