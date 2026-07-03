{{ config(materialized='table') }}

SELECT
    asset_kennung AS "Id",
    INITCAP(TRIM(asset_name)) AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    -- Warranty end date with multi-format parsing
    CASE
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN garantieende ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' 
            AND TO_DATE(TRIM(garantieende), 'YYYY-MM-DD') IS NOT NULL
            THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'M/D/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(projekt_kennung) AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    -- No source dates available for these fields
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_assets') }}
WHERE TRIM(asset_kennung) != ''