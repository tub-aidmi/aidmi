
{{ config(materialized='table') }}

SELECT
    TRIM(ma.asset_kennung) AS "Id",
    COALESCE(TRIM(ma.asset_name), 'Unnamed Asset') AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(ma.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    mk.kundennummer AS "Account__c",
    mp.projekt_kennung AS "Project__c",
    TRIM(ma.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS mk
    ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_src', 'master_projekte') }} AS mp
    ON ma.projekt_kennung = mp.projekt_kennung
