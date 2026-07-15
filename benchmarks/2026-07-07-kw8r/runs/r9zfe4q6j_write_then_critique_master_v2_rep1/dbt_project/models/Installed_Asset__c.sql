{{ config(materialized='table') }}
SELECT
    MD5(ma.asset_kennung) AS "Id",
    ma.asset_name AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende IS NULL OR ma.garantieende = 'N/A' THEN NULL
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE WHEN ma.kunden_kennung IS NOT NULL THEN MD5(ma.kunden_kennung) ELSE NULL END AS "Account__c",
    CASE WHEN ma.projekt_kennung IS NOT NULL THEN MD5(ma.projekt_kennung) ELSE NULL END AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma