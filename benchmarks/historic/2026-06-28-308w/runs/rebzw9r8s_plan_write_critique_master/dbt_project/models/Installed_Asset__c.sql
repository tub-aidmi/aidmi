
{{ config(materialized='table') }}

SELECT
    TRIM(ma.asset_kennung) AS "Id",
    COALESCE(TRIM(ma.asset_name), TRIM(ma.asset_kennung)) AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(ma.garantieende) IS NULL THEN NULL
        WHEN TRIM(ma.garantieende) IN ('0000-00-00', 'N/A', '') THEN NULL
        -- YYYY-MM-DD format
        WHEN TRIM(ma.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        -- YYYYMMDD format
        WHEN TRIM(ma.garantieende) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        -- DD.MM.YYYY format
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- MM/DD/YYYY format
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(mk.kundennummer) AS "Account__c",
    TRIM(mp.projekt_kennung) AS "Project__c",
    TRIM(ma.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS mk
    ON TRIM(ma.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN
    {{ source('fixture_master_src', 'master_projekte') }} AS mp
    ON TRIM(ma.projekt_kennung) = TRIM(mp.projekt_kennung)
