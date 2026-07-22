{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS "Id",
    COALESCE(TRIM(ma.asset_name), 'Unnamed Asset') AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(ma.garantieende, 'YYYY-MM-DD')
            WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(ma.garantieende, 'DD.MM.YYYY')
            WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(ma.garantieende, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    mk.kundennummer AS "Account__c",
    mp.projekt_kennung AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp ON ma.projekt_kennung = mp.projekt_kennung