{{ config(materialized='table') }}

SELECT
    MD5(TRIM(asset_kennung)) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    COALESCE(
        CAST(CASE
            WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende -- YYYY-MM-DD
            WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS text),
        NULL -- Warranty_End_Date__c is nullable, so NULL is fine if unparseable
    ) AS "Warranty_End_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(projekt_kennung)) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}