{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        kundennummer,
        'ACCT-' || LPAD(ROW_NUMBER() OVER (ORDER BY kust.kundennummer)::TEXT, 6, '0') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }} kust
),
project_mapping AS (
    SELECT 
        projekt_kennung,
        'PROJ-' || LPAD(ROW_NUMBER() OVER (ORDER BY proj.projekt_kennung)::TEXT, 6, '0') AS project_id
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} proj
)

SELECT
    'AST-' || LPAD(ROW_NUMBER() OVER (ORDER BY a.asset_kennung)::TEXT, 6, '0') AS "Id",
    a.asset_name AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' AND a.garantieende != '0000-00-00' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "Warranty_End_Date__c",
    am.account_id AS "Account__c",
    pm.project_id AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN account_mapping am ON a.kunden_kennung = am.kundennummer
LEFT JOIN project_mapping pm ON a.projekt_kennung = pm.projekt_kennung
