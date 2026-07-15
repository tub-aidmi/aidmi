{{ config(materialized='table') }}

SELECT
    '012' || REPLACE(a.asset_kennung, 'AST-', '') AS "Id",
    COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Untitled Asset') AS "Name",
    NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN a.kunden_kennung IS NOT NULL THEN '001' || REPLACE(a.kunden_kennung, 'CUST-M', '')
        ELSE NULL
    END AS "Account__c",
    CASE 
        WHEN a.projekt_kennung IS NOT NULL THEN '009' || REPLACE(a.projekt_kennung, 'PROJ-', '')
        ELSE NULL
    END AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
