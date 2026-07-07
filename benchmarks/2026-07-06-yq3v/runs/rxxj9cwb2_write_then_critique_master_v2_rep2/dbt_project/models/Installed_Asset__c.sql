-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    MD5(ma.asset_kennung) AS "Id",
    COALESCE(ma.asset_name, 'Unknown Asset') AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende IS NULL OR TRIM(ma.garantieende) = '' THEN NULL
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' AND ma.garantieende != '0000-00-00' THEN
            CAST(NULLIF(ma.garantieende, '') AS DATE)
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_DATE(ma.garantieende, 'DD.MM.YYYY')
        WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE(ma.garantieende, 'MM/DD/YYYY')
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c",
    MD5(mk.kundennummer) AS "Account__c",
    MD5(mp.projekt_kennung) AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON ma.projekt_kennung = mp.projekt_kennung