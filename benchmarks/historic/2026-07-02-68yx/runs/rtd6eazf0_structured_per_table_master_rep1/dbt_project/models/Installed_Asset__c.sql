{{ config(materialized='table') }}

WITH accounts AS (
    SELECT 
        '001' || LEFT(MD5(kundennummer), 15) AS sf_id,
        kundennummer AS legacy_id
    FROM {{ source('fixture_master_src', 'master_kunden') }}
),
projects AS (
    SELECT 
        'a0K' || LEFT(MD5(projekt_kennung), 15) AS sf_id,
        projekt_kennung AS legacy_id
    FROM {{ source('fixture_master_src', 'master_projekte') }}
)
SELECT 
    'a0M' || LEFT(MD5(a.asset_kennung), 15) AS "Id",
    a.asset_name AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_DATE(a.garantieende, 'YYYYMMDD')::TEXT
        WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(a.garantieende, 'DD.MM.YYYY')::TEXT
        WHEN a.garantieende ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND a.garantieende != '0000-00-00' THEN TO_DATE(a.garantieende, 'YYYY-MM-DD')::TEXT
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(a.garantieende, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.sf_id AS "Account__c",
    proj.sf_id AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_assets') }} a
LEFT JOIN accounts acc ON a.kunden_kennung = acc.legacy_id
LEFT JOIN projects proj ON a.projekt_kennung = proj.legacy_id