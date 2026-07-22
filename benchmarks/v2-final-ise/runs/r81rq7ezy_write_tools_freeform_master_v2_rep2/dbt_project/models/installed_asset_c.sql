{{ config(materialized='table') }}

SELECT
    'S' || SUBSTR(MD5(src."asset_kennung"), 1, 14) AS "Id",
    src."asset_name" AS "Name",
    src."serien_nummer" AS "Serial_Number__c",
    CASE
        WHEN src."garantieende" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."garantieende", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src."garantieende" ~ '^\d{8}$' THEN TO_CHAR(
            TO_DATE(SUBSTR(src."garantieende", 1, 4) || '-' || SUBSTR(src."garantieende", 5, 2) || '-' || SUBSTR(src."garantieende", 7, 2), 'YYYY-MM-DD'), 'YYYY-MM-DD'
        )
        ELSE NULL
    END AS "Warranty_End_Date__c",
    'A' || SUBSTR(MD5(src."kunden_kennung"), 1, 14) AS "Account__c",
    'P' || SUBSTR(MD5(src."projekt_kennung"), 1, 14) AS "Project__c",
    src."asset_kennung" AS "Legacy_Asset_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} src
