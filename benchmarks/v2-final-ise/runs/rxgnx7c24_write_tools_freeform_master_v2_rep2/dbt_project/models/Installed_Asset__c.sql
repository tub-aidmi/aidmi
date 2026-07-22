{{ config(materialized='table') }}

SELECT
    a.asset_kennung AS "Id",
    COALESCE(a.asset_name, 'Untitled Asset') AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende IS NULL OR a.garantieende = 'N/A' THEN NULL
        WHEN a.garantieende ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        ELSE NULL
    END AS "Warranty_End_Date__c",
    mk.kundennummer AS "Account__c",
    mp.projekt_kennung AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON a.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp
    ON a.projekt_kennung = mp.projekt_kennung
