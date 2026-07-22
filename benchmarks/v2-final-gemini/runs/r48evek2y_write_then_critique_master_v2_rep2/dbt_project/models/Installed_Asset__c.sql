{{ config(materialized='table') }}

SELECT
    asset.asset_kennung::text AS "Id",
    COALESCE(NULLIF(TRIM(asset.asset_name), ''), 'Unnamed Asset')::text AS "Name",
    TRIM(asset.serien_nummer)::text AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.garantieende -- Already YYYY-MM-DD
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END::text AS "Warranty_End_Date__c",
    kunden.kundennummer::text AS "Account__c",
    TRIM(projekt.projekt_kennung)::text AS "Project__c",
    asset.asset_kennung::text AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON asset.kunden_kennung = kunden.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekt
    ON asset.projekt_kennung = projekt.projekt_kennung