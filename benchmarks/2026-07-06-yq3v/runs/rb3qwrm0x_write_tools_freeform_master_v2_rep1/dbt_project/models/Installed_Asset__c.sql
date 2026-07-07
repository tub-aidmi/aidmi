{{ config(materialized='table') }}

SELECT
    a.asset_kennung AS "Id",
    COALESCE(a.asset_name, a.asset_kennung) AS "Name", -- Name is NOT NULL, use asset_kennung as fallback
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    a.kunden_kennung AS "Account__c",
    REPLACE(a.projekt_kennung, 'PROJ-M-', 'PROJ-') AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS a
