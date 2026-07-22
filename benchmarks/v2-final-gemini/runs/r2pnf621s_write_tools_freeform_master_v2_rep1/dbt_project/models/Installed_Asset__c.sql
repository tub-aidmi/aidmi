{{ config(materialized='table') }}

SELECT
    MD5(TRIM(asset_kennung)) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset ' || TRIM(asset_kennung)) AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(garantieende) ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(garantieende) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(projekt_kennung)) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }}
