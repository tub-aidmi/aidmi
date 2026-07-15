{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS "Id",
    COALESCE(NULLIF(TRIM(ma.asset_name), ''), 'Untitled Asset') AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE 
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    mk.kundennummer AS "Account__c",
    mp.projekt_kennung AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    '2023-01-01'::text AS "CreatedDate",
    '2023-01-01'::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
    ON TRIM(ma.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp 
    ON TRIM(ma.projekt_kennung) = TRIM(mp.projekt_kennung)