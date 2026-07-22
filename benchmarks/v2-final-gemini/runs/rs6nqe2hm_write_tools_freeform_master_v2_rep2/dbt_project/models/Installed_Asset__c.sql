-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    ast.asset_kennung AS "Id",
    COALESCE(ast.asset_name, 'Unknown Asset Name') AS "Name", -- Name is NOT NULL
    ast.serien_nummer AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(TO_DATE(ast.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ast.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ast.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ast.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
    ) AS "Warranty_End_Date__c",
    mak.kundennummer AS "Account__c", -- Join on kunden_kennung and kundennummer
    mproj.projekt_kennung AS "Project__c", -- Join on projekt_kennung and projekt_kennung
    ast.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ast
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mak
    ON ast.kunden_kennung = mak.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mproj
    ON ast.projekt_kennung = mproj.projekt_kennung
