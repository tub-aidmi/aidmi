-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    MD5(TRIM(asset_kennung)) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name", -- Name is NOT NULL
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SUBSTRING(garantieende FROM 1 FOR POSITION('/' IN garantieende) - 1)::INT <= 12 AND SUBSTRING(SUBSTRING(garantieende FROM POSITION('/' IN garantieende) + 1) FROM 1 FOR POSITION('/' IN garantieende) - 1)::INT <= 31 THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(projekt_kennung)) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }}
WHERE
    asset_kennung IS NOT NULL;
