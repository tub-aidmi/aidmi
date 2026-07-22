{{ config(materialized='table') }}
SELECT
    ma.asset_kennung AS "Id",
    COALESCE(NULLIF(TRIM(ma.asset_name), ''), 'Unknown Asset') AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende IS NULL OR TRIM(ma.garantieende) IN ('N/A', '0000-00-00') THEN NULL
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE
        WHEN mk.kundennummer IS NOT NULL THEN CONCAT('ACCOUNT-', REPLACE(mk.kundennummer, 'CUST-', ''))
        ELSE NULL
    END AS "Account__c",
    mp.projekt_kennung AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp ON ma.projekt_kennung = mp.projekt_kennung