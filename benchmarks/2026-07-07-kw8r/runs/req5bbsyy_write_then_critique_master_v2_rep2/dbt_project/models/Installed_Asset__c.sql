{{ config(materialized='table') }}

SELECT
    a.asset_kennung AS "Id",
    COALESCE(TRIM(a.asset_name), 'Unknown') AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende IS NULL OR TRIM(a.garantieende) = '' THEN NULL
        WHEN a.garantieende ILIKE '%na%' THEN NULL
        WHEN a.garantieende ~ '^0{4}-0{2}-0{2}$' THEN NULL
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || LPAD(REGEXP_REPLACE(a.kunden_kennung, '[^0-9]', ''), 9, '0') AS "Account__c",
    'P_' || REPLACE(a.projekt_kennung, 'PROJ-', '') AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a