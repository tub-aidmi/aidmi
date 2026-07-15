{{ config(materialized='table') }}

WITH account_ids AS (
    SELECT 
        kundennummer,
        MD5(kundennummer) AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
project_ids AS (
    SELECT 
        projekt_kennung,
        MD5(projekt_kennung) AS project_id
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT 
    MD5(a.asset_kennung) AS "Id",
    COALESCE(TRIM(a.asset_name), '') AS "Name",
    TRIM(a.serien_nummer) AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende IS NULL OR a.garantieende = 'N/A' THEN NULL
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ac.account_id AS "Account__c",
    pj.project_id AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN account_ids ac ON a.kunden_kennung = ac.kundennummer
LEFT JOIN project_ids pj ON a.projekt_kennung = pj.projekt_kennung