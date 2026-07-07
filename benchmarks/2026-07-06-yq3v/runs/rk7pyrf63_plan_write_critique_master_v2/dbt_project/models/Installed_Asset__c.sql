-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    MD5(TRIM(ma.asset_kennung)) AS "Id",
    COALESCE(TRIM(ma.asset_name), 'Unknown Asset') AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(MD5(TRIM(mk.kundennummer)), MD5('UNKNOWN_ACCOUNT')) AS "Account__c",
    COALESCE(MD5(TRIM(mp.projekt_kennung)), MD5('UNKNOWN_PROJECT')) AS "Project__c",
    TRIM(ma.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON TRIM(ma.kunden_kennung) = TRIM(mk.kundennummer)
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON TRIM(ma.projekt_kennung) = TRIM(mp.projekt_kennung)