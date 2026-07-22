-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(asset.asset_kennung) AS "Id",
    COALESCE(asset.asset_name, 'Unknown Asset') AS "Name",
    asset.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.garantieende
        WHEN asset.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(cust.kundennummer) AS "Account__c",
    MD5(proj.projekt_kennung) AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS cust
    ON asset.kunden_kennung = cust.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS proj
    ON asset.projekt_kennung = proj.projekt_kennung;