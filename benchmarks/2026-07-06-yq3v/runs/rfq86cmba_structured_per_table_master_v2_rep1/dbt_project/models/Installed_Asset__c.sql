{{ config(materialized='table') }}

SELECT
    MD5(ma.asset_kennung) AS "Id",
    COALESCE(TRIM(ma.asset_name), TRIM(ma.asset_kennung)) AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(mk.kundennummer) AS "Account__c",
    MD5(mp.projekt_kennung) AS "Project__c",
    TRIM(ma.asset_kennung) AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON TRIM(ma.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON TRIM(ma.projekt_kennung) = TRIM(mp.projekt_kennung)
