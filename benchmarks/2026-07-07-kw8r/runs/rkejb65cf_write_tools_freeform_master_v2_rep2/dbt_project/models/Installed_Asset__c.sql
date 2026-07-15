{{ config(materialized='table') }}

SELECT
    MD5(asset_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(asset_name), ''), 'Untitled Asset') AS "Name",
    NULLIF(TRIM(serien_nummer), '') AS "Serial_Number__c",
    CASE 
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN kunden_kennung IS NOT NULL THEN MD5(kunden_kennung)
        ELSE NULL
    END AS "Account__c",
    CASE 
        WHEN projekt_kennung IS NOT NULL THEN MD5(projekt_kennung)
        ELSE NULL
    END AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}
